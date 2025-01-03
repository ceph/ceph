======
Quincy
======

Quincy is the 17th stable release of Ceph. It is named after Squidward
Quincy Tentacles from Spongebob Squarepants.

v17.2.8 Quincy
==============

This is the eighth, and expected to be last, backport release in the Quincy series. We recommend
that all users update to this release.

v17.2.8 will have RPM/centos 9 packages instead of RPM/centos 8 built.

v17.2.8 container images, now based on CentOS 9, may be incompatible on older kernels (e.g., Ubuntu 18.04) 
due to differences in thread creation methods. 
Users upgrading to v17.2.8 container images with older OS versions may encounter crashes during `pthread_create`. 
However, we recommend upgrading your OS to avoid this unsupported combination.

Users should expect to see the el8 rpm subdirectory empty and the "dnf" commands are expected
to fail with 17.2.8.
They can choose to use 17.2.8 RPM packages for centos 8/el8 provided by CERN as a community
member or continue to stay at 17.2.7 following instructions
from https://docs.ceph.com/en/latest/install/get-packages/#rhel, the ceph.repo file should
point to https://download.ceph.com/rpm-17.2.7/el8 instead of https://download.ceph.com/rpm-quincy/el8

These CERN packages come with no warranty and have not been tested. The software in them has been
tested by Ceph according to `platforms <https://docs.ceph.com/en/latest/start/os-recommendations/#platforms>`_ .
The repository for el8 builds is hosted by CERN on `Linux@CERN <https://linuxsoft.cern.ch/repos/ceph-ext-quincy8el-stable/>`_ .
The public part of the GPG key used to sign the
packages is available at `RPM-GPG-KEY-Ceph-Community <https://linuxsoft.cern.ch/repos/RPM-GPG-KEY-Ceph-Community>`_ .

Notable Changes
---------------

* RADOS: `get_pool_is_selfmanaged_snaps_mode` C++ API has been deprecated
  due to being prone to false negative results.  Its safer replacement is
  `pool_is_in_selfmanaged_snaps_mode`.
* RBD: When diffing against the beginning of time (`fromsnapname == NULL`) in
  fast-diff mode (`whole_object == true` with the `fast-diff` image feature enabled
  and valid), diff-iterate is now guaranteed to execute locally if exclusive
  lock is available.  This brings a dramatic performance improvement for QEMU
  live disk synchronization and backup use cases.
* RBD: The option ``--image-id`` has been added to the `rbd children` CLI command,
  so it can be run for images in the trash.
* RBD: `RBD_IMAGE_OPTION_CLONE_FORMAT` option has been exposed in Python
  bindings via the `clone_format` optional parameter to `clone`, `deep_copy` and
  `migration_prepare` methods.
* RBD: `RBD_IMAGE_OPTION_FLATTEN` option has been exposed in Python bindings via
  `flatten` optional parameter to the `deep_copy` and `migration_prepare` methods.

Changelog
---------

* .github: sync the list of paths for rbd label, expand tests label to qa/\* (`pr#57726 <https://github.com/ceph/ceph/pull/57726>`_, Ilya Dryomov)
* [quincy] qa/multisite: stabilize multisite testing (`pr#60479 <https://github.com/ceph/ceph/pull/60479>`_, Shilpa Jagannath, Casey Bodley)
* [quincy] RGW backports (`pr#51806 <https://github.com/ceph/ceph/pull/51806>`_, Soumya Koduri, Casey Bodley)
* [rgw][lc][rgw_lifecycle_work_time] adjust timing if the configured end time is less than the start time (`pr#54874 <https://github.com/ceph/ceph/pull/54874>`_, Oguzhan Ozmen)
* Add Containerfile and build.sh to build it (`pr#60230 <https://github.com/ceph/ceph/pull/60230>`_, Dan Mick)
* admin/doc-requirements: bump Sphinx to 5.0.2 (`pr#55204 <https://github.com/ceph/ceph/pull/55204>`_, Nizamudeen A)
* batch backport of #50743,  #55342, #48557 (`pr#55593 <https://github.com/ceph/ceph/pull/55593>`_, John Mulligan, Afreen, Laura Flores)
* blk/aio: fix long batch (64+K entries) submission (`pr#58674 <https://github.com/ceph/ceph/pull/58674>`_, Igor Fedotov, Adam Kupczyk, Robin Geuze)
* bluestore/bluestore_types: avoid heap-buffer-overflow in another way to keep code uniformity (`pr#58818 <https://github.com/ceph/ceph/pull/58818>`_, Rongqi Sun)
* bluestore/bluestore_types: check 'it' valid before using (`pr#56889 <https://github.com/ceph/ceph/pull/56889>`_, Rongqi Sun)
* build: Make boost_url a list (`pr#58316 <https://github.com/ceph/ceph/pull/58316>`_, Adam Emerson, Kefu Chai)
* centos 9 related backports for RBD (`pr#58565 <https://github.com/ceph/ceph/pull/58565>`_, Casey Bodley, Ilya Dryomov)
* ceph-menv:fix typo in README (`pr#55164 <https://github.com/ceph/ceph/pull/55164>`_, yu.wang)
* ceph-node-proxy not present, not part of container (`pr#60337 <https://github.com/ceph/ceph/pull/60337>`_, Dan Mick)
* ceph-volume: add missing import (`pr#56260 <https://github.com/ceph/ceph/pull/56260>`_, Guillaume Abrioux)
* ceph-volume: create LVs when using partitions (`pr#58221 <https://github.com/ceph/ceph/pull/58221>`_, Guillaume Abrioux)
* ceph-volume: fix a bug in _check_generic_reject_reasons (`pr#54706 <https://github.com/ceph/ceph/pull/54706>`_, Kim Minjong)
* ceph-volume: fix a regression in `raw list` (`pr#54522 <https://github.com/ceph/ceph/pull/54522>`_, Guillaume Abrioux)
* ceph-volume: Fix migration from WAL to data with no DB (`pr#55496 <https://github.com/ceph/ceph/pull/55496>`_, Igor Fedotov)
* ceph-volume: Fix unbound var in disk.get_devices() (`pr#59651 <https://github.com/ceph/ceph/pull/59651>`_, Zack Cerza)
* ceph-volume: fix zap_partitions() in devices.lvm.zap (`pr#55480 <https://github.com/ceph/ceph/pull/55480>`_, Guillaume Abrioux)
* ceph-volume: fixes fallback to stat in is_device and is_partition (`pr#54630 <https://github.com/ceph/ceph/pull/54630>`_, Teoman ONAY)
* ceph-volume: Revert "ceph-volume: fix raw list for lvm devices" (`pr#54430 <https://github.com/ceph/ceph/pull/54430>`_, Matthew Booth, Guillaume Abrioux)
* ceph-volume: use 'no workqueue' options with dmcrypt (`pr#55336 <https://github.com/ceph/ceph/pull/55336>`_, Guillaume Abrioux)
* ceph-volume: use importlib from stdlib on Python 3.8 and up (`pr#58006 <https://github.com/ceph/ceph/pull/58006>`_, Guillaume Abrioux, Kefu Chai)
* ceph-volume: Use safe accessor to get TYPE info (`pr#56322 <https://github.com/ceph/ceph/pull/56322>`_, Dillon Amburgey)
* ceph.spec.in: add support for openEuler OS (`pr#56366 <https://github.com/ceph/ceph/pull/56366>`_, liuqinfei)
* ceph.spec.in: we need jsonnet for all distroes for make check (`pr#60074 <https://github.com/ceph/ceph/pull/60074>`_, Kyr Shatskyy)
* ceph_test_rados_api_misc: adjust LibRadosMiscConnectFailure.ConnectTimeout timeout (`pr#58128 <https://github.com/ceph/ceph/pull/58128>`_, Lucian Petrut)
* cephadm: add a --dry-run option to cephadm shell (`pr#54221 <https://github.com/ceph/ceph/pull/54221>`_, John Mulligan)
* cephadm: add tcmu-runner to logrotate config (`pr#55966 <https://github.com/ceph/ceph/pull/55966>`_, Adam King)
* cephadm: add timemaster to timesync services list (`pr#56308 <https://github.com/ceph/ceph/pull/56308>`_, Florent Carli)
* cephadm: Adding support to configure public_network cfg section (`pr#55959 <https://github.com/ceph/ceph/pull/55959>`_, Redouane Kachach)
* cephadm: allow ports to be opened in firewall during adoption, reconfig, redeploy (`pr#55960 <https://github.com/ceph/ceph/pull/55960>`_, Adam King)
* cephadm: disable ms_bind_ipv4 if we will enable ms_bind_ipv6 (`pr#58760 <https://github.com/ceph/ceph/pull/58760>`_, Dan van der Ster, Joshua Blanch)
* cephadm: fix host-maintenance command always exiting with a failure (`pr#58755 <https://github.com/ceph/ceph/pull/58755>`_, John Mulligan)
* cephadm: make custom_configs work for tcmu-runner container (`pr#53425 <https://github.com/ceph/ceph/pull/53425>`_, Adam King)
* cephadm: pin pyfakefs version for tox tests (`pr#56763 <https://github.com/ceph/ceph/pull/56763>`_, Adam King)
* cephadm: remove restriction for crush device classes (`pr#56087 <https://github.com/ceph/ceph/pull/56087>`_, Seena Fallah)
* cephadm: run tcmu-runner through script to do restart on failure (`pr#55975 <https://github.com/ceph/ceph/pull/55975>`_, Adam King, Raimund Sacherer, Teoman ONAY, Ilya Dryomov)
* cephadm: support for CA signed keys (`pr#55965 <https://github.com/ceph/ceph/pull/55965>`_, Adam King)
* cephadm: turn off cgroups_split setting  when bootstrapping with --no-cgroups-split (`pr#58761 <https://github.com/ceph/ceph/pull/58761>`_, Adam King)
* cephadm: use importlib.metadata for querying ceph_iscsi's version (`pr#58637 <https://github.com/ceph/ceph/pull/58637>`_, Kefu Chai)
* cephfs-mirror: various fixes (`pr#56702 <https://github.com/ceph/ceph/pull/56702>`_, Jos Collin)
* cephfs: Fixed a bug in the readdir_cache_cb function that may have us… (`pr#58806 <https://github.com/ceph/ceph/pull/58806>`_, Tod Chen)
* cephfs: upgrade cephfs-shell's path wherever necessary (`pr#54186 <https://github.com/ceph/ceph/pull/54186>`_, Rishabh Dave)
* client, mds: update mtime and change attr for snapdir when snaps are created, deleted and renamed (`issue#54501 <http://tracker.ceph.com/issues/54501>`_, `pr#50730 <https://github.com/ceph/ceph/pull/50730>`_, Venky Shankar)
* client/fuse: handle case of renameat2 with non-zero flags (`pr#55010 <https://github.com/ceph/ceph/pull/55010>`_, Leonid Usov, Shachar Sharon)
* client: always refresh mds feature bits on session open (`issue#63188 <http://tracker.ceph.com/issues/63188>`_, `pr#54244 <https://github.com/ceph/ceph/pull/54244>`_, Venky Shankar)
* client: call _getattr() for -ENODATA returned _getvxattr() calls (`pr#54405 <https://github.com/ceph/ceph/pull/54405>`_, Jos Collin)
* client: disallow unprivileged users to escalate root privileges (`pr#60314 <https://github.com/ceph/ceph/pull/60314>`_, Xiubo Li, Venky Shankar)
* client: fix leak of file handles (`pr#56121 <https://github.com/ceph/ceph/pull/56121>`_, Xavi Hernandez)
* client: queue a delay cap flushing if there are ditry caps/snapcaps (`pr#54465 <https://github.com/ceph/ceph/pull/54465>`_, Xiubo Li)
* cloud sync: fix crash due to objs on cr stack (`pr#51136 <https://github.com/ceph/ceph/pull/51136>`_, Yehuda Sadeh)
* cls/cas/cls_cas_internal: Initialize 'hash' value before decoding (`pr#59236 <https://github.com/ceph/ceph/pull/59236>`_, Nitzan Mordechai)
* cmake/modules/BuildRocksDB.cmake: inherit parent's CMAKE_CXX_FLAGS (`pr#55501 <https://github.com/ceph/ceph/pull/55501>`_, Kefu Chai)
* cmake/rgw: librgw tests depend on ALLOC_LIBS (`pr#54796 <https://github.com/ceph/ceph/pull/54796>`_, Casey Bodley)
* cmake: use or turn off liburing for rocksdb (`pr#54123 <https://github.com/ceph/ceph/pull/54123>`_, Casey Bodley, Patrick Donnelly)
* common/admin_socket: add a command to raise a signal (`pr#54356 <https://github.com/ceph/ceph/pull/54356>`_, Leonid Usov)
* common/dout: fix FTBFS on GCC 14 (`pr#59057 <https://github.com/ceph/ceph/pull/59057>`_, Radoslaw Zarzynski)
* common/Formatter: dump inf/nan as null (`pr#60064 <https://github.com/ceph/ceph/pull/60064>`_, Md Mahamudur Rahaman Sajib)
* common/StackStringStream: update pointer to newly allocated memory in overflow() (`pr#57363 <https://github.com/ceph/ceph/pull/57363>`_, Rongqi Sun)
* common/weighted_shuffle: don't feed std::discrete_distribution with all-zero weights (`pr#55154 <https://github.com/ceph/ceph/pull/55154>`_, Radosław Zarzyński)
* common:  intrusive_lru destructor add (`pr#54557 <https://github.com/ceph/ceph/pull/54557>`_, Ali Maredia)
* common: fix compilation warnings in numa.cc (`pr#58704 <https://github.com/ceph/ceph/pull/58704>`_, Radoslaw Zarzynski)
* common: resolve config proxy deadlock using refcounted pointers (`pr#54374 <https://github.com/ceph/ceph/pull/54374>`_, Patrick Donnelly)
* Do not duplicate query-string in ops-log (`pr#57132 <https://github.com/ceph/ceph/pull/57132>`_, Matt Benjamin)
* do not evict clients if OSDs are laggy (`pr#52271 <https://github.com/ceph/ceph/pull/52271>`_, Dhairya Parmar, Laura Flores)
* doc/architecture.rst - fix typo (`pr#55385 <https://github.com/ceph/ceph/pull/55385>`_, Zac Dover)
* doc/architecture.rst: improve rados definition (`pr#55344 <https://github.com/ceph/ceph/pull/55344>`_, Zac Dover)
* doc/architecture: correct typo (`pr#56013 <https://github.com/ceph/ceph/pull/56013>`_, Zac Dover)
* doc/architecture: improve some paragraphs (`pr#55400 <https://github.com/ceph/ceph/pull/55400>`_, Zac Dover)
* doc/architecture: remove pleonasm (`pr#55934 <https://github.com/ceph/ceph/pull/55934>`_, Zac Dover)
* doc/ceph-volume: add spillover fix procedure (`pr#59542 <https://github.com/ceph/ceph/pull/59542>`_, Zac Dover)
* doc/ceph-volume: explain idempotence (`pr#54234 <https://github.com/ceph/ceph/pull/54234>`_, Zac Dover)
* doc/ceph-volume: improve front matter (`pr#54236 <https://github.com/ceph/ceph/pull/54236>`_, Zac Dover)
* doc/cephadm - edit t11ing (`pr#55483 <https://github.com/ceph/ceph/pull/55483>`_, Zac Dover)
* doc/cephadm/services: remove excess rendered indentation in osd.rst (`pr#54324 <https://github.com/ceph/ceph/pull/54324>`_, Ville Ojamo)
* doc/cephadm/upgrade: ceph-ci containers are hosted by quay.ceph.io (`pr#58682 <https://github.com/ceph/ceph/pull/58682>`_, Casey Bodley)
* doc/cephadm: add default monitor images (`pr#57210 <https://github.com/ceph/ceph/pull/57210>`_, Zac Dover)
* doc/cephadm: add malformed-JSON removal instructions (`pr#59665 <https://github.com/ceph/ceph/pull/59665>`_, Zac Dover)
* doc/cephadm: add note about ceph-exporter (Quincy) (`pr#55520 <https://github.com/ceph/ceph/pull/55520>`_, Zac Dover)
* doc/cephadm: correct nfs config pool name (`pr#55604 <https://github.com/ceph/ceph/pull/55604>`_, Zac Dover)
* doc/cephadm: edit "Using Custom Images" (`pr#58942 <https://github.com/ceph/ceph/pull/58942>`_, Zac Dover)
* doc/cephadm: edit troubleshooting.rst (1 of x) (`pr#54284 <https://github.com/ceph/ceph/pull/54284>`_, Zac Dover)
* doc/cephadm: edit troubleshooting.rst (2 of x) (`pr#54321 <https://github.com/ceph/ceph/pull/54321>`_, Zac Dover)
* doc/cephadm: explain different methods of cephadm delivery (`pr#56176 <https://github.com/ceph/ceph/pull/56176>`_, Zac Dover)
* doc/cephadm: fix typo in set ssh key command (`pr#54389 <https://github.com/ceph/ceph/pull/54389>`_, Piotr Parczewski)
* doc/cephadm: how to get exact size_spec from device (`pr#59432 <https://github.com/ceph/ceph/pull/59432>`_, Zac Dover)
* doc/cephadm: improve host-management.rst (`pr#56112 <https://github.com/ceph/ceph/pull/56112>`_, Anthony D'Atri)
* doc/cephadm: Improve multiple files (`pr#56134 <https://github.com/ceph/ceph/pull/56134>`_, Anthony D'Atri)
* doc/cephadm: Quincy default images procedure (`pr#57239 <https://github.com/ceph/ceph/pull/57239>`_, Zac Dover)
* doc/cephadm: remove downgrade reference from upgrade docs (`pr#57087 <https://github.com/ceph/ceph/pull/57087>`_, Adam King)
* doc/cephfs/client-auth.rst: correct fs authorize cephfs1 /dir1 clie… (`pr#55247 <https://github.com/ceph/ceph/pull/55247>`_, 叶海丰)
* doc/cephfs: add cache pressure information (`pr#59150 <https://github.com/ceph/ceph/pull/59150>`_, Zac Dover)
* doc/cephfs: add doc for disabling mgr/volumes plugin (`pr#60498 <https://github.com/ceph/ceph/pull/60498>`_, Rishabh Dave)
* doc/cephfs: disambiguate "Reporting Free Space" (`pr#56873 <https://github.com/ceph/ceph/pull/56873>`_, Zac Dover)
* doc/cephfs: disambiguate two sentences (`pr#57705 <https://github.com/ceph/ceph/pull/57705>`_, Zac Dover)
* doc/cephfs: edit "Cloning Snapshots" in fs-volumes.rst (`pr#57667 <https://github.com/ceph/ceph/pull/57667>`_, Zac Dover)
* doc/cephfs: edit "is mount helper present" (`pr#58580 <https://github.com/ceph/ceph/pull/58580>`_, Zac Dover)
* doc/cephfs: edit "Layout Fields" text (`pr#59023 <https://github.com/ceph/ceph/pull/59023>`_, Zac Dover)
* doc/cephfs: edit "Pinning Subvolumes..." (`pr#57664 <https://github.com/ceph/ceph/pull/57664>`_, Zac Dover)
* doc/cephfs: edit add-remove-mds (`pr#55649 <https://github.com/ceph/ceph/pull/55649>`_, Zac Dover)
* doc/cephfs: edit front matter in client-auth.rst (`pr#57123 <https://github.com/ceph/ceph/pull/57123>`_, Zac Dover)
* doc/cephfs: edit front matter in mantle.rst (`pr#57793 <https://github.com/ceph/ceph/pull/57793>`_, Zac Dover)
* doc/cephfs: edit fs-volumes.rst (1 of x) (`pr#57419 <https://github.com/ceph/ceph/pull/57419>`_, Zac Dover)
* doc/cephfs: edit fs-volumes.rst (1 of x) followup (`pr#57428 <https://github.com/ceph/ceph/pull/57428>`_, Zac Dover)
* doc/cephfs: edit fs-volumes.rst (2 of x) (`pr#57544 <https://github.com/ceph/ceph/pull/57544>`_, Zac Dover)
* doc/cephfs: edit mount-using-fuse.rst (`pr#54354 <https://github.com/ceph/ceph/pull/54354>`_, Jaanus Torp)
* doc/cephfs: edit vstart warning text (`pr#57816 <https://github.com/ceph/ceph/pull/57816>`_, Zac Dover)
* doc/cephfs: fix "file layouts" link (`pr#58877 <https://github.com/ceph/ceph/pull/58877>`_, Zac Dover)
* doc/cephfs: fix "OSD capabilities" link (`pr#58894 <https://github.com/ceph/ceph/pull/58894>`_, Zac Dover)
* doc/cephfs: fix architecture link to correct relative path (`pr#56341 <https://github.com/ceph/ceph/pull/56341>`_, molpako)
* doc/cephfs: improve "layout fields" text (`pr#59252 <https://github.com/ceph/ceph/pull/59252>`_, Zac Dover)
* doc/cephfs: improve cache-configuration.rst (`pr#59216 <https://github.com/ceph/ceph/pull/59216>`_, Zac Dover)
* doc/cephfs: improve ceph-fuse command (`pr#56969 <https://github.com/ceph/ceph/pull/56969>`_, Zac Dover)
* doc/cephfs: note regarding start time time zone (`pr#53577 <https://github.com/ceph/ceph/pull/53577>`_, Milind Changire)
* doc/cephfs: rearrange subvolume group information (`pr#60437 <https://github.com/ceph/ceph/pull/60437>`_, Indira Sawant)
* doc/cephfs: refine client-auth (1 of 3) (`pr#56781 <https://github.com/ceph/ceph/pull/56781>`_, Zac Dover)
* doc/cephfs: refine client-auth (2 of 3) (`pr#56843 <https://github.com/ceph/ceph/pull/56843>`_, Zac Dover)
* doc/cephfs: refine client-auth (3 of 3) (`pr#56852 <https://github.com/ceph/ceph/pull/56852>`_, Zac Dover)
* doc/cephfs: s/mountpoint/mount point/ (`pr#59296 <https://github.com/ceph/ceph/pull/59296>`_, Zac Dover)
* doc/cephfs: s/mountpoint/mount point/ (`pr#59288 <https://github.com/ceph/ceph/pull/59288>`_, Zac Dover)
* doc/cephfs: s/subvolumegroups/subvolume groups (`pr#57744 <https://github.com/ceph/ceph/pull/57744>`_, Zac Dover)
* doc/cephfs: separate commands into sections (`pr#57670 <https://github.com/ceph/ceph/pull/57670>`_, Zac Dover)
* doc/cephfs: streamline a paragraph (`pr#58776 <https://github.com/ceph/ceph/pull/58776>`_, Zac Dover)
* doc/cephfs: take Anthony's suggestion (`pr#58361 <https://github.com/ceph/ceph/pull/58361>`_, Zac Dover)
* doc/cephfs: update cephfs-shell link (`pr#58372 <https://github.com/ceph/ceph/pull/58372>`_, Zac Dover)
* doc/cephfs: Update disaster-recovery-experts.rst to mention Slack (`pr#55045 <https://github.com/ceph/ceph/pull/55045>`_, Dhairya Parmar)
* doc/cephfs: use 'p' flag to set layouts or quotas (`pr#60484 <https://github.com/ceph/ceph/pull/60484>`_, TruongSinh Tran-Nguyen)
* doc/config: edit "ceph-conf.rst" (`pr#54464 <https://github.com/ceph/ceph/pull/54464>`_, Zac Dover)
* doc/dev/peering: Change acting set num (`pr#59064 <https://github.com/ceph/ceph/pull/59064>`_, qn2060)
* doc/dev/release-process.rst: note new 'project' arguments (`pr#57645 <https://github.com/ceph/ceph/pull/57645>`_, Dan Mick)
* doc/dev: add "activate latest release" RTD step (`pr#59656 <https://github.com/ceph/ceph/pull/59656>`_, Zac Dover)
* doc/dev: add formatting to basic workflow (`pr#58739 <https://github.com/ceph/ceph/pull/58739>`_, Zac Dover)
* doc/dev: edit "Principles for format change" (`pr#58577 <https://github.com/ceph/ceph/pull/58577>`_, Zac Dover)
* doc/dev: edit internals.rst (`pr#55853 <https://github.com/ceph/ceph/pull/55853>`_, Zac Dover)
* doc/dev: fix spelling in crimson.rst (`pr#55738 <https://github.com/ceph/ceph/pull/55738>`_, Zac Dover)
* doc/dev: Fix typos in encoding.rst (`pr#58306 <https://github.com/ceph/ceph/pull/58306>`_, N Balachandran)
* doc/dev: improve basic-workflow.rst (`pr#58939 <https://github.com/ceph/ceph/pull/58939>`_, Zac Dover)
* doc/dev: link to ceph.io leads list (`pr#58107 <https://github.com/ceph/ceph/pull/58107>`_, Zac Dover)
* doc/dev: osd_internals/snaps.rst: add clone_overlap doc (`pr#56524 <https://github.com/ceph/ceph/pull/56524>`_, Matan Breizman)
* doc/dev: refine "Concepts" (`pr#56661 <https://github.com/ceph/ceph/pull/56661>`_, Zac Dover)
* doc/dev: refine "Concepts" 2 of 3 (`pr#56726 <https://github.com/ceph/ceph/pull/56726>`_, Zac Dover)
* doc/dev: refine "Concepts" 3 of 3 (`pr#56730 <https://github.com/ceph/ceph/pull/56730>`_, Zac Dover)
* doc/dev: refine "Concepts" 4 of 3 (`pr#56741 <https://github.com/ceph/ceph/pull/56741>`_, Zac Dover)
* doc/dev: remove "Stable Releases and Backports" (`pr#60274 <https://github.com/ceph/ceph/pull/60274>`_, Zac Dover)
* doc/dev: repair broken image (`pr#57009 <https://github.com/ceph/ceph/pull/57009>`_, Zac Dover)
* doc/dev: s/to asses/to assess/ (`pr#57424 <https://github.com/ceph/ceph/pull/57424>`_, Zac Dover)
* doc/dev: update leads list (`pr#56604 <https://github.com/ceph/ceph/pull/56604>`_, Zac Dover)
* doc/dev: update leads list (`pr#56590 <https://github.com/ceph/ceph/pull/56590>`_, Zac Dover)
* doc/dev_guide: add needs-upgrade-testing label info (`pr#58731 <https://github.com/ceph/ceph/pull/58731>`_, Zac Dover)
* doc/developer_guide: update doc about installing teuthology (`pr#57751 <https://github.com/ceph/ceph/pull/57751>`_, Rishabh Dave)
* doc/glossary.rst: add "Monitor Store" (`pr#54744 <https://github.com/ceph/ceph/pull/54744>`_, Zac Dover)
* doc/glossary.rst: add "OpenStack Swift" and "Swift" (`pr#57943 <https://github.com/ceph/ceph/pull/57943>`_, Zac Dover)
* doc/glossary: add "ceph-ansible" (`pr#59009 <https://github.com/ceph/ceph/pull/59009>`_, Zac Dover)
* doc/glossary: add "ceph-fuse" entry (`pr#58945 <https://github.com/ceph/ceph/pull/58945>`_, Zac Dover)
* doc/glossary: add "Crimson" entry (`pr#56074 <https://github.com/ceph/ceph/pull/56074>`_, Zac Dover)
* doc/glossary: add "librados" entry (`pr#56236 <https://github.com/ceph/ceph/pull/56236>`_, Zac Dover)
* doc/glossary: add "object storage" (`pr#59426 <https://github.com/ceph/ceph/pull/59426>`_, Zac Dover)
* doc/glossary: Add "OMAP" to glossary (`pr#55750 <https://github.com/ceph/ceph/pull/55750>`_, Zac Dover)
* doc/glossary: add "PLP" to glossary (`pr#60505 <https://github.com/ceph/ceph/pull/60505>`_, Zac Dover)
* doc/glossary: add "Prometheus" (`pr#58979 <https://github.com/ceph/ceph/pull/58979>`_, Zac Dover)
* doc/glossary: add "Quorum" to glossary (`pr#54510 <https://github.com/ceph/ceph/pull/54510>`_, Zac Dover)
* doc/glossary: Add "S3" (`pr#57984 <https://github.com/ceph/ceph/pull/57984>`_, Zac Dover)
* doc/glossary: Add link to CRUSH paper (`pr#55558 <https://github.com/ceph/ceph/pull/55558>`_, Zac Dover)
* doc/glossary: improve "BlueStore" entry (`pr#54266 <https://github.com/ceph/ceph/pull/54266>`_, Zac Dover)
* doc/glossary: improve "MDS" entry (`pr#55850 <https://github.com/ceph/ceph/pull/55850>`_, Zac Dover)
* doc/glossary: improve OSD definitions (`pr#55614 <https://github.com/ceph/ceph/pull/55614>`_, Zac Dover)
* doc/governance: add Zac Dover's updated email (`pr#60136 <https://github.com/ceph/ceph/pull/60136>`_, Zac Dover)
* doc/install: add manual RADOSGW install procedure (`pr#55881 <https://github.com/ceph/ceph/pull/55881>`_, Zac Dover)
* doc/install: fix typos in openEuler-installation doc (`pr#56414 <https://github.com/ceph/ceph/pull/56414>`_, Rongqi Sun)
* doc/install: Keep the name field of the created user consistent with … (`pr#59758 <https://github.com/ceph/ceph/pull/59758>`_, hejindong)
* doc/install: update "update submodules" (`pr#54962 <https://github.com/ceph/ceph/pull/54962>`_, Zac Dover)
* doc/man/8/mount.ceph.rst: add more mount options (`pr#55755 <https://github.com/ceph/ceph/pull/55755>`_, Xiubo Li)
* doc/man/8/radosgw-admin: add get lifecycle command (`pr#57161 <https://github.com/ceph/ceph/pull/57161>`_, rkhudov)
* doc/man: add missing long option switches (`pr#57708 <https://github.com/ceph/ceph/pull/57708>`_, Patrick Donnelly)
* doc/man: edit "manipulating the omap key" (`pr#55636 <https://github.com/ceph/ceph/pull/55636>`_, Zac Dover)
* doc/man: edit ceph-bluestore-tool.rst (`pr#59684 <https://github.com/ceph/ceph/pull/59684>`_, Zac Dover)
* doc/man: edit ceph-osd description (`pr#54552 <https://github.com/ceph/ceph/pull/54552>`_, Zac Dover)
* doc/man: supplant "wsync" with "nowsync" as the default (`pr#60201 <https://github.com/ceph/ceph/pull/60201>`_, Zac Dover)
* doc/mds: improve wording (`pr#59587 <https://github.com/ceph/ceph/pull/59587>`_, Piotr Parczewski)
* doc/mgr/dashboard: fix TLS typo (`pr#59033 <https://github.com/ceph/ceph/pull/59033>`_, Mindy Preston)
* doc/mgr: credit John Jasen for Zabbix 2 (`pr#56685 <https://github.com/ceph/ceph/pull/56685>`_, Zac Dover)
* doc/mgr: document lack of MSWin NFS 4.x support (`pr#55033 <https://github.com/ceph/ceph/pull/55033>`_, Zac Dover)
* doc/mgr: edit "Overview" in dashboard.rst (`pr#57337 <https://github.com/ceph/ceph/pull/57337>`_, Zac Dover)
* doc/mgr: edit "Resolve IP address to hostname before redirect" (`pr#57297 <https://github.com/ceph/ceph/pull/57297>`_, Zac Dover)
* doc/mgr: explain error message - dashboard.rst (`pr#57110 <https://github.com/ceph/ceph/pull/57110>`_, Zac Dover)
* doc/mgr: remove ceph-exporter (Quincy) (`pr#55518 <https://github.com/ceph/ceph/pull/55518>`_, Zac Dover)
* doc/mgr: remove Zabbix 1 information (`pr#56799 <https://github.com/ceph/ceph/pull/56799>`_, Zac Dover)
* doc/mgr: update zabbix information (`pr#56632 <https://github.com/ceph/ceph/pull/56632>`_, Zac Dover)
* doc/rados/configuration/bluestore-config-ref: Fix lowcase typo (`pr#54695 <https://github.com/ceph/ceph/pull/54695>`_, Adam Kupczyk)
* doc/rados/configuration/osd-config-ref: fix typo (`pr#55679 <https://github.com/ceph/ceph/pull/55679>`_, Pierre Riteau)
* doc/rados/operations: add EC overhead table to erasure-code.rst (`pr#55245 <https://github.com/ceph/ceph/pull/55245>`_, Anthony D'Atri)
* doc/rados/operations: document `ceph balancer status detail` (`pr#55264 <https://github.com/ceph/ceph/pull/55264>`_, Laura Flores)
* doc/rados/operations: Fix off-by-one errors in control.rst (`pr#55232 <https://github.com/ceph/ceph/pull/55232>`_, tobydarling)
* doc/rados/operations: Improve crush_location docs (`pr#56595 <https://github.com/ceph/ceph/pull/56595>`_, Niklas Hambüchen)
* doc/rados/operations: Improve health-checks.rst (`pr#59584 <https://github.com/ceph/ceph/pull/59584>`_, Anthony D'Atri)
* doc/rados/operations: remove vanity cluster name reference from crush… (`pr#58949 <https://github.com/ceph/ceph/pull/58949>`_, Anthony D'Atri)
* doc/rados/operations: rephrase OSDs peering (`pr#57158 <https://github.com/ceph/ceph/pull/57158>`_, Piotr Parczewski)
* doc/rados: add "change public network" procedure (`pr#55800 <https://github.com/ceph/ceph/pull/55800>`_, Zac Dover)
* doc/rados: add "pgs not deep scrubbed in time" info (`pr#59735 <https://github.com/ceph/ceph/pull/59735>`_, Zac Dover)
* doc/rados: add bucket rename command (`pr#57028 <https://github.com/ceph/ceph/pull/57028>`_, Zac Dover)
* doc/rados: add confval directives to health-checks (`pr#59873 <https://github.com/ceph/ceph/pull/59873>`_, Zac Dover)
* doc/rados: add link to messenger v2 info in mon-lookup-dns.rst (`pr#59796 <https://github.com/ceph/ceph/pull/59796>`_, Zac Dover)
* doc/rados: add link to pg blog post (`pr#55612 <https://github.com/ceph/ceph/pull/55612>`_, Zac Dover)
* doc/rados: add options to network config ref (`pr#57917 <https://github.com/ceph/ceph/pull/57917>`_, Zac Dover)
* doc/rados: add osd_deep_scrub_interval setting operation (`pr#59804 <https://github.com/ceph/ceph/pull/59804>`_, Zac Dover)
* doc/rados: add PG definition (`pr#55631 <https://github.com/ceph/ceph/pull/55631>`_, Zac Dover)
* doc/rados: add pg-states and pg-concepts to tree (`pr#58051 <https://github.com/ceph/ceph/pull/58051>`_, Zac Dover)
* doc/rados: add stop monitor command (`pr#57852 <https://github.com/ceph/ceph/pull/57852>`_, Zac Dover)
* doc/rados: add stretch_rule workaround (`pr#58183 <https://github.com/ceph/ceph/pull/58183>`_, Zac Dover)
* doc/rados: credit Prashant for a procedure (`pr#58259 <https://github.com/ceph/ceph/pull/58259>`_, Zac Dover)
* doc/rados: document manually passing search domain (`pr#58433 <https://github.com/ceph/ceph/pull/58433>`_, Zac Dover)
* doc/rados: document unfound object cache-tiering scenario (`pr#59382 <https://github.com/ceph/ceph/pull/59382>`_, Zac Dover)
* doc/rados: edit "client can't connect..." (`pr#54655 <https://github.com/ceph/ceph/pull/54655>`_, Zac Dover)
* doc/rados: edit "Everything Failed! Now What?" (`pr#54666 <https://github.com/ceph/ceph/pull/54666>`_, Zac Dover)
* doc/rados: edit "monitor store failures" (`pr#54660 <https://github.com/ceph/ceph/pull/54660>`_, Zac Dover)
* doc/rados: edit "Placement Groups Never Get Clean" (`pr#60048 <https://github.com/ceph/ceph/pull/60048>`_, Zac Dover)
* doc/rados: edit "recovering broken monmap" (`pr#54602 <https://github.com/ceph/ceph/pull/54602>`_, Zac Dover)
* doc/rados: edit "troubleshooting-mon" (`pr#54503 <https://github.com/ceph/ceph/pull/54503>`_, Zac Dover)
* doc/rados: edit "understanding mon_status" (`pr#54580 <https://github.com/ceph/ceph/pull/54580>`_, Zac Dover)
* doc/rados: edit "Using the Monitor's Admin Socket" (`pr#54577 <https://github.com/ceph/ceph/pull/54577>`_, Zac Dover)
* doc/rados: edit t-mon "common issues" (1 of x) (`pr#54419 <https://github.com/ceph/ceph/pull/54419>`_, Zac Dover)
* doc/rados: edit t-mon "common issues" (2 of x) (`pr#54422 <https://github.com/ceph/ceph/pull/54422>`_, Zac Dover)
* doc/rados: edit t-mon "common issues" (3 of x) (`pr#54439 <https://github.com/ceph/ceph/pull/54439>`_, Zac Dover)
* doc/rados: edit t-mon "common issues" (4 of x) (`pr#54444 <https://github.com/ceph/ceph/pull/54444>`_, Zac Dover)
* doc/rados: edit t-mon "common issues" (5 of x) (`pr#54456 <https://github.com/ceph/ceph/pull/54456>`_, Zac Dover)
* doc/rados: edit t-mon.rst text (`pr#54350 <https://github.com/ceph/ceph/pull/54350>`_, Zac Dover)
* doc/rados: edit t-shooting-mon.rst (`pr#54428 <https://github.com/ceph/ceph/pull/54428>`_, Zac Dover)
* doc/rados: edit troubleshooting-osd.rst (`pr#58273 <https://github.com/ceph/ceph/pull/58273>`_, Zac Dover)
* doc/rados: edit troubleshooting-pg.rst (`pr#54229 <https://github.com/ceph/ceph/pull/54229>`_, Zac Dover)
* doc/rados: explain replaceable parts of command (`pr#58061 <https://github.com/ceph/ceph/pull/58061>`_, Zac Dover)
* doc/rados: fix broken links (`pr#55681 <https://github.com/ceph/ceph/pull/55681>`_, Zac Dover)
* doc/rados: fix outdated value for ms_bind_port_max (`pr#57049 <https://github.com/ceph/ceph/pull/57049>`_, Pierre Riteau)
* doc/rados: followup to PR#58057 (`pr#58163 <https://github.com/ceph/ceph/pull/58163>`_, Zac Dover)
* doc/rados: format "initial troubleshooting" (`pr#54478 <https://github.com/ceph/ceph/pull/54478>`_, Zac Dover)
* doc/rados: format Q&A list in t-mon.rst (`pr#54346 <https://github.com/ceph/ceph/pull/54346>`_, Zac Dover)
* doc/rados: format Q&A list in tshooting-mon.rst (`pr#54367 <https://github.com/ceph/ceph/pull/54367>`_, Zac Dover)
* doc/rados: format sections in tshooting-mon.rst (`pr#54639 <https://github.com/ceph/ceph/pull/54639>`_, Zac Dover)
* doc/rados: improve "Ceph Subsystems" (`pr#54703 <https://github.com/ceph/ceph/pull/54703>`_, Zac Dover)
* doc/rados: improve "scrubbing" explanation (`pr#54271 <https://github.com/ceph/ceph/pull/54271>`_, Zac Dover)
* doc/rados: improve formatting of log-and-debug.rst (`pr#54747 <https://github.com/ceph/ceph/pull/54747>`_, Zac Dover)
* doc/rados: improve leader/peon monitor explanation (`pr#57960 <https://github.com/ceph/ceph/pull/57960>`_, Zac Dover)
* doc/rados: link to pg setting commands (`pr#55937 <https://github.com/ceph/ceph/pull/55937>`_, Zac Dover)
* doc/rados: ops/pgs: s/power of 2/power of two (`pr#54701 <https://github.com/ceph/ceph/pull/54701>`_, Zac Dover)
* doc/rados: parallelize t-mon headings (`pr#54462 <https://github.com/ceph/ceph/pull/54462>`_, Zac Dover)
* doc/rados: PR#57022 unfinished business (`pr#57266 <https://github.com/ceph/ceph/pull/57266>`_, Zac Dover)
* doc/rados: remove dual-stack docs (`pr#57074 <https://github.com/ceph/ceph/pull/57074>`_, Zac Dover)
* doc/rados: remove PGcalc from docs (`pr#55902 <https://github.com/ceph/ceph/pull/55902>`_, Zac Dover)
* doc/rados: remove redundant pg repair commands (`pr#57041 <https://github.com/ceph/ceph/pull/57041>`_, Zac Dover)
* doc/rados: repair stretch-mode.rst (`pr#54763 <https://github.com/ceph/ceph/pull/54763>`_, Zac Dover)
* doc/rados: restore PGcalc tool (`pr#56058 <https://github.com/ceph/ceph/pull/56058>`_, Zac Dover)
* doc/rados: revert "doc/rados/operations: document `ceph balancer status detail`" (`pr#55359 <https://github.com/ceph/ceph/pull/55359>`_, Laura Flores)
* doc/rados: s/cepgsqlite/cephsqlite/ (`pr#57248 <https://github.com/ceph/ceph/pull/57248>`_, Zac Dover)
* doc/rados: standardize markup of "clean" (`pr#60502 <https://github.com/ceph/ceph/pull/60502>`_, Zac Dover)
* doc/rados: update "stretch mode" (`pr#54757 <https://github.com/ceph/ceph/pull/54757>`_, Michael Collins)
* doc/rados: update common.rst (`pr#56269 <https://github.com/ceph/ceph/pull/56269>`_, Zac Dover)
* doc/rados: update config for autoscaler (`pr#55439 <https://github.com/ceph/ceph/pull/55439>`_, Zac Dover)
* doc/rados: update how to install c++ header files (`pr#58309 <https://github.com/ceph/ceph/pull/58309>`_, Pere Diaz Bou)
* doc/rados: update PG guidance (`pr#55461 <https://github.com/ceph/ceph/pull/55461>`_, Zac Dover)
* doc/radosgw - edit admin.rst "set user rate limit" (`pr#55151 <https://github.com/ceph/ceph/pull/55151>`_, Zac Dover)
* doc/radosgw/admin.rst: use underscores in config var names (`pr#54934 <https://github.com/ceph/ceph/pull/54934>`_, Ville Ojamo)
* doc/radosgw/multisite: fix Configuring Secondary Zones -> Updating the Period (`pr#60334 <https://github.com/ceph/ceph/pull/60334>`_, Casey Bodley)
* doc/radosgw: add confval directives (`pr#55485 <https://github.com/ceph/ceph/pull/55485>`_, Zac Dover)
* doc/radosgw: add gateway starting command (`pr#54834 <https://github.com/ceph/ceph/pull/54834>`_, Zac Dover)
* doc/radosgw: admin.rst - edit "Create a Subuser" (`pr#55021 <https://github.com/ceph/ceph/pull/55021>`_, Zac Dover)
* doc/radosgw: admin.rst - edit "Create a User" (`pr#55005 <https://github.com/ceph/ceph/pull/55005>`_, Zac Dover)
* doc/radosgw: admin.rst - edit sections (`pr#55018 <https://github.com/ceph/ceph/pull/55018>`_, Zac Dover)
* doc/radosgw: disambiguate version-added remarks (`pr#57142 <https://github.com/ceph/ceph/pull/57142>`_, Zac Dover)
* doc/radosgw: edit "Add/Remove a Key" (`pr#55056 <https://github.com/ceph/ceph/pull/55056>`_, Zac Dover)
* doc/radosgw: edit "Enable/Disable Bucket Rate Limit" (`pr#55261 <https://github.com/ceph/ceph/pull/55261>`_, Zac Dover)
* doc/radosgw: edit "read/write global rate limit" admin.rst (`pr#55272 <https://github.com/ceph/ceph/pull/55272>`_, Zac Dover)
* doc/radosgw: edit "remove a subuser" (`pr#55035 <https://github.com/ceph/ceph/pull/55035>`_, Zac Dover)
* doc/radosgw: edit "Usage" admin.rst (`pr#55322 <https://github.com/ceph/ceph/pull/55322>`_, Zac Dover)
* doc/radosgw: edit admin.rst "Get Bucket Rate Limit" (`pr#55254 <https://github.com/ceph/ceph/pull/55254>`_, Zac Dover)
* doc/radosgw: edit admin.rst "get user rate limit" (`pr#55158 <https://github.com/ceph/ceph/pull/55158>`_, Zac Dover)
* doc/radosgw: edit admin.rst "set bucket rate limit" (`pr#55243 <https://github.com/ceph/ceph/pull/55243>`_, Zac Dover)
* doc/radosgw: edit admin.rst - quota (`pr#55083 <https://github.com/ceph/ceph/pull/55083>`_, Zac Dover)
* doc/radosgw: edit admin.rst 1 of x (`pr#55001 <https://github.com/ceph/ceph/pull/55001>`_, Zac Dover)
* doc/radosgw: edit compression.rst (`pr#54986 <https://github.com/ceph/ceph/pull/54986>`_, Zac Dover)
* doc/radosgw: edit front matter - role.rst (`pr#54855 <https://github.com/ceph/ceph/pull/54855>`_, Zac Dover)
* doc/radosgw: edit multisite.rst (`pr#55672 <https://github.com/ceph/ceph/pull/55672>`_, Zac Dover)
* doc/radosgw: edit sections (`pr#55028 <https://github.com/ceph/ceph/pull/55028>`_, Zac Dover)
* doc/radosgw: fix formatting (`pr#54754 <https://github.com/ceph/ceph/pull/54754>`_, Zac Dover)
* doc/radosgw: Fix JSON typo in Principal Tag example code snippet (`pr#54643 <https://github.com/ceph/ceph/pull/54643>`_, Daniel Parkes)
* doc/radosgw: fix verb disagreement - index.html (`pr#55339 <https://github.com/ceph/ceph/pull/55339>`_, Zac Dover)
* doc/radosgw: format "Create a Role" (`pr#54887 <https://github.com/ceph/ceph/pull/54887>`_, Zac Dover)
* doc/radosgw: format commands in role.rst (`pr#54906 <https://github.com/ceph/ceph/pull/54906>`_, Zac Dover)
* doc/radosgw: format POST statements (`pr#54850 <https://github.com/ceph/ceph/pull/54850>`_, Zac Dover)
* doc/radosgw: Improve dynamicresharding.rst (`pr#54369 <https://github.com/ceph/ceph/pull/54369>`_, Anthony D'Atri)
* doc/radosgw: Revert "doc/rgw/lua: add info uploading a (`pr#55526 <https://github.com/ceph/ceph/pull/55526>`_, Zac Dover)
* doc/radosgw: update link in rgw-cache.rst (`pr#54806 <https://github.com/ceph/ceph/pull/54806>`_, Zac Dover)
* doc/radosgw: update S3 action list (`pr#57366 <https://github.com/ceph/ceph/pull/57366>`_, Zac Dover)
* doc/radosgw: use 'confval' directive for reshard config options (`pr#57025 <https://github.com/ceph/ceph/pull/57025>`_, Casey Bodley)
* doc/radosrgw: edit admin.rst (`pr#55074 <https://github.com/ceph/ceph/pull/55074>`_, Zac Dover)
* doc/rbd/rbd-exclusive-locks: mention incompatibility with advisory locks (`pr#58865 <https://github.com/ceph/ceph/pull/58865>`_, Ilya Dryomov)
* doc/rbd: "rbd flatten" doesn't take encryption options in quincy (`pr#56272 <https://github.com/ceph/ceph/pull/56272>`_, Ilya Dryomov)
* doc/rbd: add namespace information for mirror commands (`pr#60271 <https://github.com/ceph/ceph/pull/60271>`_, N Balachandran)
* doc/rbd: minor changes to the rbd man page (`pr#56257 <https://github.com/ceph/ceph/pull/56257>`_, N Balachandran)
* doc/README.md - add ordered list (`pr#59800 <https://github.com/ceph/ceph/pull/59800>`_, Zac Dover)
* doc/README.md: create selectable commands (`pr#59836 <https://github.com/ceph/ceph/pull/59836>`_, Zac Dover)
* doc/README.md: edit "Build Prerequisites" (`pr#59639 <https://github.com/ceph/ceph/pull/59639>`_, Zac Dover)
* doc/README.md: improve formatting (`pr#59702 <https://github.com/ceph/ceph/pull/59702>`_, Zac Dover)
* doc/rgw/d3n: pass cache dir volume to extra_container_args (`pr#59769 <https://github.com/ceph/ceph/pull/59769>`_, Mark Kogan)
* doc/rgw/notification: persistent notification queue full behavior (`pr#59235 <https://github.com/ceph/ceph/pull/59235>`_, Yuval Lifshitz)
* doc/rgw/notifications: specify which event types are enabled by default (`pr#54501 <https://github.com/ceph/ceph/pull/54501>`_, Yuval Lifshitz)
* doc/rgw: edit admin.rst - rate limit management (`pr#55129 <https://github.com/ceph/ceph/pull/55129>`_, Zac Dover)
* doc/rgw: fix Attributes index in CreateTopic example (`pr#55433 <https://github.com/ceph/ceph/pull/55433>`_, Casey Bodley)
* doc/security: remove old GPG information (`pr#56915 <https://github.com/ceph/ceph/pull/56915>`_, Zac Dover)
* doc/security: update CVE list (`pr#57019 <https://github.com/ceph/ceph/pull/57019>`_, Zac Dover)
* doc/src: add inline literals (``) to variables (`pr#57938 <https://github.com/ceph/ceph/pull/57938>`_, Zac Dover)
* doc/src: invadvisable is not a word (`pr#58191 <https://github.com/ceph/ceph/pull/58191>`_, Doug Whitfield)
* doc/start: Add Beginner's Guide (`pr#57823 <https://github.com/ceph/ceph/pull/57823>`_, Zac Dover)
* doc/start: add links to Beginner's Guide (`pr#58204 <https://github.com/ceph/ceph/pull/58204>`_, Zac Dover)
* doc/start: add Slack invite link (`pr#56042 <https://github.com/ceph/ceph/pull/56042>`_, Zac Dover)
* doc/start: add vstart install guide (`pr#60463 <https://github.com/ceph/ceph/pull/60463>`_, Zac Dover)
* doc/start: Edit Beginner's Guide (`pr#57846 <https://github.com/ceph/ceph/pull/57846>`_, Zac Dover)
* doc/start: explain "OSD" (`pr#54560 <https://github.com/ceph/ceph/pull/54560>`_, Zac Dover)
* doc/start: fix typo in hardware-recommendations.rst (`pr#54481 <https://github.com/ceph/ceph/pull/54481>`_, Anthony D'Atri)
* doc/start: fix wording & syntax (`pr#58365 <https://github.com/ceph/ceph/pull/58365>`_, Piotr Parczewski)
* doc/start: improve MDS explanation (`pr#56467 <https://github.com/ceph/ceph/pull/56467>`_, Zac Dover)
* doc/start: improve MDS explanation (`pr#56427 <https://github.com/ceph/ceph/pull/56427>`_, Zac Dover)
* doc/start: link to mon map command (`pr#56411 <https://github.com/ceph/ceph/pull/56411>`_, Zac Dover)
* doc/start: remove "intro.rst" (`pr#57950 <https://github.com/ceph/ceph/pull/57950>`_, Zac Dover)
* doc/start: remove mention of Centos 8 support (`pr#58391 <https://github.com/ceph/ceph/pull/58391>`_, Zac Dover)
* doc/start: s/http/https/ in links (`pr#57872 <https://github.com/ceph/ceph/pull/57872>`_, Zac Dover)
* doc/start: s/intro.rst/index.rst/ (`pr#57904 <https://github.com/ceph/ceph/pull/57904>`_, Zac Dover)
* doc/start: update mailing list links (`pr#58685 <https://github.com/ceph/ceph/pull/58685>`_, Zac Dover)
* doc/start: update release names (`pr#54573 <https://github.com/ceph/ceph/pull/54573>`_, Zac Dover)
* doc: add description of metric fields for cephfs-top (`pr#55512 <https://github.com/ceph/ceph/pull/55512>`_, Neeraj Pratap Singh)
* doc: add supported file types in cephfs-mirroring.rst (`pr#54823 <https://github.com/ceph/ceph/pull/54823>`_, Jos Collin)
* doc: Amend dev mailing list subscribe instructions (`pr#58698 <https://github.com/ceph/ceph/pull/58698>`_, Paulo E. Castro)
* doc: cephadm/services/osd: fix typo (`pr#56231 <https://github.com/ceph/ceph/pull/56231>`_, Lorenz Bausch)
* doc: clarify availability vs integrity (`pr#58132 <https://github.com/ceph/ceph/pull/58132>`_, Gregory O'Neill)
* doc: clarify superuser note for ceph-fuse (`pr#58616 <https://github.com/ceph/ceph/pull/58616>`_, Patrick Donnelly)
* doc: clarify use of location: in host spec (`pr#57648 <https://github.com/ceph/ceph/pull/57648>`_, Matthew Vernon)
* doc: Correct link to "Device management" (`pr#58490 <https://github.com/ceph/ceph/pull/58490>`_, Matthew Vernon)
* doc: Correct link to Prometheus docs (`pr#59561 <https://github.com/ceph/ceph/pull/59561>`_, Matthew Vernon)
* doc: correct typo (`pr#57885 <https://github.com/ceph/ceph/pull/57885>`_, Matthew Vernon)
* doc: discuss the standard multi-tenant CephFS security model (`pr#53559 <https://github.com/ceph/ceph/pull/53559>`_, Greg Farnum)
* doc: Document the Windows CI job (`pr#60035 <https://github.com/ceph/ceph/pull/60035>`_, Lucian Petrut)
* doc: documenting the feature that scrub clear the entries from damage… (`pr#59080 <https://github.com/ceph/ceph/pull/59080>`_, Neeraj Pratap Singh)
* doc: explain the consequence of enabling mirroring through monitor co… (`pr#60527 <https://github.com/ceph/ceph/pull/60527>`_, Jos Collin)
* doc: fix email (`pr#60235 <https://github.com/ceph/ceph/pull/60235>`_, Ernesto Puerta)
* doc: fix typo (`pr#59993 <https://github.com/ceph/ceph/pull/59993>`_, N Balachandran)
* doc: Fixes two typos and grammatical errors. Signed-off-by: Sina Ahma… (`pr#54776 <https://github.com/ceph/ceph/pull/54776>`_, Sina Ahmadi)
* doc: Improve doc/radosgw/placement.rst (`pr#58975 <https://github.com/ceph/ceph/pull/58975>`_, Anthony D'Atri)
* doc: specify correct fs type for mkfs (`pr#55283 <https://github.com/ceph/ceph/pull/55283>`_, Vladislav Glagolev)
* doc: SubmittingPatches-backports - remove backports team (`pr#60299 <https://github.com/ceph/ceph/pull/60299>`_, Zac Dover)
* doc: Update "Getting Started" to link to start not install (`pr#59909 <https://github.com/ceph/ceph/pull/59909>`_, Matthew Vernon)
* doc: Update dynamicresharding.rst (`pr#54330 <https://github.com/ceph/ceph/pull/54330>`_, Aliaksei Makarau)
* doc: update rgw admin api req params for get user info (`pr#55072 <https://github.com/ceph/ceph/pull/55072>`_, Ali Maredia)
* doc: update tests-integration-testing-teuthology-workflow.rst (`pr#59550 <https://github.com/ceph/ceph/pull/59550>`_, Vallari Agrawal)
* doc:start.rst fix typo in hw-recs (`pr#55506 <https://github.com/ceph/ceph/pull/55506>`_, Eduardo Roldan)
* doc:update e-mail addresses governance (`pr#60086 <https://github.com/ceph/ceph/pull/60086>`_, Tobias Fischer)
* docs/rados/operations/stretch-mode: warn device class is not supported (`pr#59101 <https://github.com/ceph/ceph/pull/59101>`_, Kamoltat Sirivadhna)
* docs/rados: remove incorrect ceph command (`pr#56496 <https://github.com/ceph/ceph/pull/56496>`_, Taha Jahangir)
* docs/radosgw: edit admin.rst "enable/disable user rate limit" (`pr#55195 <https://github.com/ceph/ceph/pull/55195>`_, Zac Dover)
* docs/rbd: fix typo in arg name (`pr#56263 <https://github.com/ceph/ceph/pull/56263>`_, N Balachandran)
* docs: Add information about OpenNebula integration (`pr#54939 <https://github.com/ceph/ceph/pull/54939>`_, Daniel Clavijo)
* docs: removed centos 8 and added squid to the build matrix (`pr#58903 <https://github.com/ceph/ceph/pull/58903>`_, Yuri Weinstein)
* global: Call getnam_r with a 64KiB buffer on the heap (`pr#60124 <https://github.com/ceph/ceph/pull/60124>`_, Adam Emerson)
* install-deps.sh, do_cmake.sh: almalinux is another el flavour (`pr#58523 <https://github.com/ceph/ceph/pull/58523>`_, Dan van der Ster)
* install-deps: save and restore user's XDG_CACHE_HOME (`pr#56991 <https://github.com/ceph/ceph/pull/56991>`_, luo rixin)
* kv/RocksDBStore: Configure compact-on-deletion for all CFs (`pr#57404 <https://github.com/ceph/ceph/pull/57404>`_, Joshua Baergen)
* librados: make querying pools for selfmanaged snaps reliable (`pr#55025 <https://github.com/ceph/ceph/pull/55025>`_, Ilya Dryomov)
* librados: use CEPH_OSD_FLAG_FULL_FORCE for IoCtxImpl::remove (`pr#59283 <https://github.com/ceph/ceph/pull/59283>`_, Chen Yuanrun)
* librbd/crypto: fix issue when live-migrating from encrypted export (`pr#59144 <https://github.com/ceph/ceph/pull/59144>`_, Ilya Dryomov)
* librbd/migration: prune snapshot extents in RawFormat::list_snaps() (`pr#59659 <https://github.com/ceph/ceph/pull/59659>`_, Ilya Dryomov)
* librbd: account for discards that truncate in ObjectListSnapsRequest (`pr#56212 <https://github.com/ceph/ceph/pull/56212>`_, Ilya Dryomov)
* librbd: Append one journal event per image request (`pr#54819 <https://github.com/ceph/ceph/pull/54819>`_, Ilya Dryomov, Joshua Baergen)
* librbd: create rbd_trash object during pool initialization and namespace creation (`pr#57604 <https://github.com/ceph/ceph/pull/57604>`_, Ramana Raja)
* librbd: diff-iterate shouldn't crash on an empty byte range (`pr#58210 <https://github.com/ceph/ceph/pull/58210>`_, Ilya Dryomov)
* librbd: disallow group snap rollback if memberships don't match (`pr#58208 <https://github.com/ceph/ceph/pull/58208>`_, Ilya Dryomov)
* librbd: don't crash on a zero-length read if buffer is NULL (`pr#57569 <https://github.com/ceph/ceph/pull/57569>`_, Ilya Dryomov)
* librbd: don't report HOLE_UPDATED when diffing against a hole (`pr#54950 <https://github.com/ceph/ceph/pull/54950>`_, Ilya Dryomov)
* librbd: fix regressions in ObjectListSnapsRequest (`pr#54861 <https://github.com/ceph/ceph/pull/54861>`_, Ilya Dryomov)
* librbd: fix split() for SparseExtent and SparseBufferlistExtent (`pr#55664 <https://github.com/ceph/ceph/pull/55664>`_, Ilya Dryomov)
* librbd: improve rbd_diff_iterate2() performance in fast-diff mode (`pr#55257 <https://github.com/ceph/ceph/pull/55257>`_, Ilya Dryomov)
* librbd: make diff-iterate in fast-diff mode aware of encryption (`pr#58342 <https://github.com/ceph/ceph/pull/58342>`_, Ilya Dryomov)
* librbd: make group and group snapshot IDs more random (`pr#57090 <https://github.com/ceph/ceph/pull/57090>`_, Ilya Dryomov)
* librbd: return ENOENT from Snapshot::get_timestamp for nonexistent snap_id (`pr#55473 <https://github.com/ceph/ceph/pull/55473>`_, John Agombar)
* librgw: teach librgw about rgw_backend_store (`pr#59315 <https://github.com/ceph/ceph/pull/59315>`_, Matt Benjamin)
* log: Make log_max_recent have an effect again (`pr#48310 <https://github.com/ceph/ceph/pull/48310>`_, Joshua Baergen)
* make-dist: don't use --continue option for wget (`pr#55092 <https://github.com/ceph/ceph/pull/55092>`_, Casey Bodley)
* MClientRequest: properly handle ceph_mds_request_head_legacy for ext_num_retry, ext_num_fwd, owner_uid, owner_gid (`pr#54411 <https://github.com/ceph/ceph/pull/54411>`_, Alexander Mikhalitsyn)
* mds,qa: some balancer debug messages (<=5) not printed when debug_mds is >=5 (`pr#53551 <https://github.com/ceph/ceph/pull/53551>`_, Patrick Donnelly)
* mds/MDBalancer: ignore queued callbacks if MDS is not active (`pr#54494 <https://github.com/ceph/ceph/pull/54494>`_, Leonid Usov)
* mds/MDSRank: Add set_history_slow_op_size_and_threshold for op_tracker (`pr#53358 <https://github.com/ceph/ceph/pull/53358>`_, Yite Gu)
* mds: add a command to dump directory information (`pr#55986 <https://github.com/ceph/ceph/pull/55986>`_, Jos Collin, Zhansong Gao)
* mds: add debug logs during setxattr ceph.dir.subvolume (`pr#56061 <https://github.com/ceph/ceph/pull/56061>`_, Milind Changire)
* mds: adjust pre_segments_size for MDLog when trimming segments for st… (`issue#59833 <http://tracker.ceph.com/issues/59833>`_, `pr#54034 <https://github.com/ceph/ceph/pull/54034>`_, Venky Shankar)
* mds: allow lock state to be LOCK_MIX_SYNC in replica for filelock (`pr#56050 <https://github.com/ceph/ceph/pull/56050>`_, Xiubo Li)
* mds: change priority of mds rss perf counter to useful (`pr#55058 <https://github.com/ceph/ceph/pull/55058>`_, sp98)
* mds: disable `defer_client_eviction_on_laggy_osds' by default (`issue#64685 <http://tracker.ceph.com/issues/64685>`_, `pr#56195 <https://github.com/ceph/ceph/pull/56195>`_, Venky Shankar)
* mds: do not simplify fragset (`pr#54892 <https://github.com/ceph/ceph/pull/54892>`_, Milind Changire)
* mds: do remove the cap when seqs equal or larger than last issue (`pr#58296 <https://github.com/ceph/ceph/pull/58296>`_, Xiubo Li)
* mds: dump locks when printing mutation ops (`pr#52976 <https://github.com/ceph/ceph/pull/52976>`_, Patrick Donnelly)
* mds: ensure next replay is queued on req drop (`pr#54315 <https://github.com/ceph/ceph/pull/54315>`_, Patrick Donnelly)
* mds: fix session/client evict command (`issue#68132 <http://tracker.ceph.com/issues/68132>`_, `pr#58724 <https://github.com/ceph/ceph/pull/58724>`_, Venky Shankar, Neeraj Pratap Singh)
* mds: log message when exiting due to asok command (`pr#53549 <https://github.com/ceph/ceph/pull/53549>`_, Patrick Donnelly)
* mds: prevent scrubbing for standby-replay MDS (`pr#58799 <https://github.com/ceph/ceph/pull/58799>`_, Neeraj Pratap Singh)
* mds: replacing bootstrap session only if handle client session message (`pr#53363 <https://github.com/ceph/ceph/pull/53363>`_, Mer Xuanyi)
* mds: revert standby-replay trimming changes (`pr#54717 <https://github.com/ceph/ceph/pull/54717>`_, Patrick Donnelly)
* mds: set the correct WRLOCK flag always in wrlock_force() (`pr#58773 <https://github.com/ceph/ceph/pull/58773>`_, Xiubo Li)
* mds: set the loner to true for LOCK_EXCL_XSYN (`pr#54910 <https://github.com/ceph/ceph/pull/54910>`_, Xiubo Li)
* mds: try to choose a new batch head in request_clientup() (`pr#58843 <https://github.com/ceph/ceph/pull/58843>`_, Xiubo Li)
* mds: use variable g_ceph_context directly in MDSAuthCaps (`pr#52820 <https://github.com/ceph/ceph/pull/52820>`_, Rishabh Dave)
* MDSAuthCaps: print better error message for perm flag in MDS caps (`pr#54946 <https://github.com/ceph/ceph/pull/54946>`_, Rishabh Dave)
* mgr/BaseMgrModule: Optimize CPython Call in Finish Function (`pr#57585 <https://github.com/ceph/ceph/pull/57585>`_, Nitzan Mordechai)
* mgr/cephadm: Add "networks" parameter to orch apply rgw (`pr#55318 <https://github.com/ceph/ceph/pull/55318>`_, Teoman ONAY)
* mgr/cephadm: add "original_weight" parameter to OSD class (`pr#59412 <https://github.com/ceph/ceph/pull/59412>`_, Adam King)
* mgr/cephadm: add ability for haproxy, prometheus, grafana to bind on specific ip (`pr#58753 <https://github.com/ceph/ceph/pull/58753>`_, Adam King)
* mgr/cephadm: add is_host\_<status> functions to HostCache (`pr#55964 <https://github.com/ceph/ceph/pull/55964>`_, Adam King)
* mgr/cephadm: Adding extra arguments support for RGW frontend (`pr#55963 <https://github.com/ceph/ceph/pull/55963>`_, Adam King, Redouane Kachach)
* mgr/cephadm: allow draining host without removing conf/keyring files (`pr#55973 <https://github.com/ceph/ceph/pull/55973>`_, Adam King)
* mgr/cephadm: catch CancelledError in asyncio timeout handler (`pr#56086 <https://github.com/ceph/ceph/pull/56086>`_, Adam King)
* mgr/cephadm: ceph orch add fails when ipv6 address is surrounded by square brackets (`pr#56079 <https://github.com/ceph/ceph/pull/56079>`_, Teoman ONAY)
* mgr/cephadm: cleanup iscsi keyring upon daemon removal (`pr#58757 <https://github.com/ceph/ceph/pull/58757>`_, Adam King)
* mgr/cephadm: don't use image tag in orch upgrade ls (`pr#55974 <https://github.com/ceph/ceph/pull/55974>`_, Adam King)
* mgr/cephadm: fix flake8 test failures (`pr#58077 <https://github.com/ceph/ceph/pull/58077>`_, Nizamudeen A)
* mgr/cephadm: fix placement with label and host pattern (`pr#56088 <https://github.com/ceph/ceph/pull/56088>`_, Adam King)
* mgr/cephadm: fix reweighting of OSD when OSD removal is stopped (`pr#56083 <https://github.com/ceph/ceph/pull/56083>`_, Adam King)
* mgr/cephadm: Fix unfound progress events (`pr#58758 <https://github.com/ceph/ceph/pull/58758>`_, Prashant D)
* mgr/cephadm: fixups for asyncio based timeout (`pr#55556 <https://github.com/ceph/ceph/pull/55556>`_, Adam King)
* mgr/cephadm: make client-keyring deploying ceph.conf optional (`pr#58754 <https://github.com/ceph/ceph/pull/58754>`_, Adam King)
* mgr/cephadm: make setting --cgroups=split configurable for adopted daemons (`pr#58759 <https://github.com/ceph/ceph/pull/58759>`_, Gilad Sid)
* mgr/cephadm: pick correct IPs for ingress service based on VIP (`pr#55970 <https://github.com/ceph/ceph/pull/55970>`_, Redouane Kachach, Adam King)
* mgr/cephadm: refresh public_network for config checks before checking (`pr#56492 <https://github.com/ceph/ceph/pull/56492>`_, Adam King)
* mgr/cephadm: support for regex based host patterns (`pr#56222 <https://github.com/ceph/ceph/pull/56222>`_, Adam King)
* mgr/cephadm: support for removing host entry from crush map during host removal (`pr#56081 <https://github.com/ceph/ceph/pull/56081>`_, Adam King)
* mgr/cephadm: update timestamp on repeat daemon/service events (`pr#56080 <https://github.com/ceph/ceph/pull/56080>`_, Adam King)
* mgr/dashboard/frontend:Ceph dashboard supports multiple languages (`pr#56360 <https://github.com/ceph/ceph/pull/56360>`_, TomNewChao)
* mgr/dashboard: add Table Schema to grafonnet (`pr#56737 <https://github.com/ceph/ceph/pull/56737>`_, Aashish Sharma)
* mgr/dashboard: allow tls 1.2 with a config option (`pr#53779 <https://github.com/ceph/ceph/pull/53779>`_, Nizamudeen A)
* mgr/dashboard: change deprecated grafana URL in daemon logs (`pr#55545 <https://github.com/ceph/ceph/pull/55545>`_, Nizamudeen A)
* mgr/dashboard: Consider null values as zero in grafana panels (`pr#54540 <https://github.com/ceph/ceph/pull/54540>`_, Aashish Sharma)
* mgr/dashboard: debugging make check failure (`pr#56128 <https://github.com/ceph/ceph/pull/56128>`_, Nizamudeen A)
* mgr/dashboard: disable dashboard v3 in quincy (`pr#54250 <https://github.com/ceph/ceph/pull/54250>`_, Nizamudeen A)
* mgr/dashboard: exclude cloned-deleted RBD snaps (`pr#57221 <https://github.com/ceph/ceph/pull/57221>`_, Ernesto Puerta)
* mgr/dashboard: fix duplicate grafana panels when on mgr failover (`pr#56930 <https://github.com/ceph/ceph/pull/56930>`_, Avan Thakkar)
* mgr/dashboard: fix duplicate grafana panels when on mgr failover (`pr#56270 <https://github.com/ceph/ceph/pull/56270>`_, Avan Thakkar)
* mgr/dashboard: fix e2e failure related to landing page (`pr#55123 <https://github.com/ceph/ceph/pull/55123>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fix error while accessing roles tab when policy attached (`pr#55516 <https://github.com/ceph/ceph/pull/55516>`_, Nizamudeen A, Afreen)
* mgr/dashboard: fix rgw port manipulation error in dashboard (`pr#54176 <https://github.com/ceph/ceph/pull/54176>`_, Nizamudeen A)
* mgr/dashboard: fix the jsonschema issue in install-deps (`pr#55543 <https://github.com/ceph/ceph/pull/55543>`_, Nizamudeen A)
* mgr/dashboard: get rgw port from ssl_endpoint (`pr#55248 <https://github.com/ceph/ceph/pull/55248>`_, Nizamudeen A)
* mgr/dashboard: make ceph logo redirect to dashboard (`pr#56558 <https://github.com/ceph/ceph/pull/56558>`_, Afreen)
* mgr/dashboard: rbd image hide usage bar when disk usage is not provided (`pr#53809 <https://github.com/ceph/ceph/pull/53809>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: remove green tick on old password field (`pr#53385 <https://github.com/ceph/ceph/pull/53385>`_, Nizamudeen A)
* mgr/dashboard: remove unnecessary failing hosts e2e (`pr#53459 <https://github.com/ceph/ceph/pull/53459>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: replace deprecated table panel in grafana with a newer table panel (`pr#56680 <https://github.com/ceph/ceph/pull/56680>`_, Aashish Sharma)
* mgr/dashboard: replace piechart plugin charts with native pie chart panel (`pr#56655 <https://github.com/ceph/ceph/pull/56655>`_, Aashish Sharma)
* mgr/dashboard: rm warning/error threshold for cpu usage (`pr#56441 <https://github.com/ceph/ceph/pull/56441>`_, Nizamudeen A)
* mgr/dashboard: sanitize dashboard user creation (`pr#56551 <https://github.com/ceph/ceph/pull/56551>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: Show the OSDs Out and Down panels as red whenever an OSD is in Out or Down state in Ceph Cluster grafana dashboard (`pr#54539 <https://github.com/ceph/ceph/pull/54539>`_, Aashish Sharma)
* mgr/dashboard: upgrade from old 'graph' type panels to the new 'timeseries' panel (`pr#56653 <https://github.com/ceph/ceph/pull/56653>`_, Aashish Sharma)
* mgr/k8sevents: update V1Events to CoreV1Events (`pr#57995 <https://github.com/ceph/ceph/pull/57995>`_, Nizamudeen A)
* mgr/Mgr.cc: clear daemon health metrics instead of removing down/out osd from daemon state (`pr#58512 <https://github.com/ceph/ceph/pull/58512>`_, Cory Snyder)
* mgr/nfs: Don't crash ceph-mgr if NFS clusters are unavailable (`pr#58284 <https://github.com/ceph/ceph/pull/58284>`_, Anoop C S, Ponnuvel Palaniyappan)
* mgr/pg_autoscaler: add check for norecover flag (`pr#57568 <https://github.com/ceph/ceph/pull/57568>`_, Aishwarya Mathuria)
* mgr/prometheus: s/pkg_resources.packaging/packaging/ (`pr#58627 <https://github.com/ceph/ceph/pull/58627>`_, Adam King, Kefu Chai)
* mgr/rbd_support: fix recursive locking on CreateSnapshotRequests lock (`pr#54290 <https://github.com/ceph/ceph/pull/54290>`_, Ramana Raja)
* mgr/rest: Trim  requests array and limit size (`pr#59370 <https://github.com/ceph/ceph/pull/59370>`_, Nitzan Mordechai)
* mgr/snap_schedule: add support for monthly snapshots (`pr#54894 <https://github.com/ceph/ceph/pull/54894>`_, Milind Changire)
* mgr/snap_schedule: make fs argument mandatory if more than one filesystem exists (`pr#54090 <https://github.com/ceph/ceph/pull/54090>`_, Milind Changire)
* mgr/snap_schedule: restore yearly spec to lowercase y (`pr#57445 <https://github.com/ceph/ceph/pull/57445>`_, Milind Changire)
* mgr/snap_schedule: support subvol and group arguments (`pr#55210 <https://github.com/ceph/ceph/pull/55210>`_, Milind Changire)
* mgr/stats: initialize mx_last_updated in FSPerfStats (`pr#57442 <https://github.com/ceph/ceph/pull/57442>`_, Jos Collin)
* mgr/vol: handle case where clone index entry goes missing (`pr#58558 <https://github.com/ceph/ceph/pull/58558>`_, Rishabh Dave)
* mgr/volumes: fix `subvolume group rm` error message (`pr#54206 <https://github.com/ceph/ceph/pull/54206>`_, neeraj pratap singh, Neeraj Pratap Singh)
* mgr: add throttle policy for DaemonServer (`pr#54012 <https://github.com/ceph/ceph/pull/54012>`_, ericqzhao)
* mgr: don't dump global config holding gil (`pr#50193 <https://github.com/ceph/ceph/pull/50193>`_, Mykola Golub)
* mgr: fix a race condition in DaemonServer::handle_report() (`pr#54555 <https://github.com/ceph/ceph/pull/54555>`_, Radoslaw Zarzynski)
* mgr: remove out&down osd from mgr daemons (`pr#54534 <https://github.com/ceph/ceph/pull/54534>`_, shimin)
* mon/ConfigMonitor: Show localized name in "config dump --format json" output (`pr#53886 <https://github.com/ceph/ceph/pull/53886>`_, Sridhar Seshasayee)
* mon/ConnectionTracker.cc: disregard connection scores from mon_rank = -1 (`pr#55166 <https://github.com/ceph/ceph/pull/55166>`_, Kamoltat)
* mon/LogMonitor: Use generic cluster log level config (`pr#57521 <https://github.com/ceph/ceph/pull/57521>`_, Prashant D)
* mon/MonClient: handle ms_handle_fast_authentication return (`pr#59308 <https://github.com/ceph/ceph/pull/59308>`_, Patrick Donnelly)
* mon/Monitor: during shutdown don't accept new authentication and crea… (`pr#55597 <https://github.com/ceph/ceph/pull/55597>`_, Nitzan Mordechai)
* mon/OSDMonitor: Add force-remove-snap mon command (`pr#59403 <https://github.com/ceph/ceph/pull/59403>`_, Matan Breizman)
* mon/OSDMonitor: fix get_min_last_epoch_clean() (`pr#55868 <https://github.com/ceph/ceph/pull/55868>`_, Matan Breizman, Adam C. Emerson)
* mon/OSDMonitor: fix rmsnap command (`pr#56430 <https://github.com/ceph/ceph/pull/56430>`_, Matan Breizman)
* mon: add exception handling to ceph health mute (`pr#55117 <https://github.com/ceph/ceph/pull/55117>`_, Daniel Radjenovic)
* mon: add proxy to cache tier options (`pr#50551 <https://github.com/ceph/ceph/pull/50551>`_, tan changzhi)
* mon: fix health store size growing infinitely (`pr#55549 <https://github.com/ceph/ceph/pull/55549>`_, Wei Wang)
* mon: fix inconsistencies in class param (`pr#59278 <https://github.com/ceph/ceph/pull/59278>`_, Victoria Mackie)
* mon: fix mds metadata lost in one case (`pr#54317 <https://github.com/ceph/ceph/pull/54317>`_, shimin)
* mon: stuck peering since warning is misleading (`pr#57407 <https://github.com/ceph/ceph/pull/57407>`_, shreyanshjain7174)
* msg/async: Encode message once features are set (`pr#59442 <https://github.com/ceph/ceph/pull/59442>`_, Aishwarya Mathuria)
* msg/AsyncMessenger: re-evaluate the stop condition when woken up in 'wait()' (`pr#53718 <https://github.com/ceph/ceph/pull/53718>`_, Leonid Usov)
* msg: update MOSDOp() to use ceph_tid_t instead of long (`pr#55425 <https://github.com/ceph/ceph/pull/55425>`_, Lucian Petrut)
* nofail option in fstab not supported (`pr#52986 <https://github.com/ceph/ceph/pull/52986>`_, Leonid Usov)
* os/bluestore: allow use BtreeAllocator (`pr#59498 <https://github.com/ceph/ceph/pull/59498>`_, tan changzhi)
* os/bluestore: enable async manual compactions (`pr#58742 <https://github.com/ceph/ceph/pull/58742>`_, Igor Fedotov)
* os/bluestore: expand BlueFS log if available space is insufficient (`pr#57243 <https://github.com/ceph/ceph/pull/57243>`_, Pere Diaz Bou)
* os/bluestore: fix crash caused by dividing by 0 (`pr#57198 <https://github.com/ceph/ceph/pull/57198>`_, Jrchyang Yu)
* os/bluestore: fix free space update after bdev-expand in NCB mode (`pr#55776 <https://github.com/ceph/ceph/pull/55776>`_, Igor Fedotov)
* os/bluestore: fix the problem of l_bluefs_log_compactions double recording (`pr#57196 <https://github.com/ceph/ceph/pull/57196>`_, Wang Linke)
* os/bluestore: get rid off resulting lba alignment in allocators (`pr#54877 <https://github.com/ceph/ceph/pull/54877>`_, Igor Fedotov)
* os/bluestore: set rocksdb iterator bounds for Bluestore::_collection_list() (`pr#57622 <https://github.com/ceph/ceph/pull/57622>`_, Cory Snyder)
* os/bluestore: Warning added for slow operations and stalled read (`pr#59468 <https://github.com/ceph/ceph/pull/59468>`_, Md Mahamudur Rahaman Sajib)
* os/store_test: Retune tests to current code (`pr#56138 <https://github.com/ceph/ceph/pull/56138>`_, Adam Kupczyk)
* os: introduce ObjectStore::refresh_perf_counters() method (`pr#55133 <https://github.com/ceph/ceph/pull/55133>`_, Igor Fedotov)
* osd/ECTransaction: Remove incorrect asserts in generate_transactions (`pr#59132 <https://github.com/ceph/ceph/pull/59132>`_, Mark Nelson)
* osd/OSD: introduce reset_purged_snaps_last (`pr#53973 <https://github.com/ceph/ceph/pull/53973>`_, Matan Breizman)
* osd/OSDMap: Check for uneven weights & != 2 buckets post stretch mode (`pr#52458 <https://github.com/ceph/ceph/pull/52458>`_, Kamoltat)
* osd/scrub: increasing max_osd_scrubs to 3 (`pr#55174 <https://github.com/ceph/ceph/pull/55174>`_, Ronen Friedman)
* osd/SnapMapper: fix _lookup_purged_snap (`pr#56815 <https://github.com/ceph/ceph/pull/56815>`_, Matan Breizman)
* osd/TrackedOp: Fix TrackedOp event order (`pr#59109 <https://github.com/ceph/ceph/pull/59109>`_, YiteGu)
* osd: always send returnvec-on-errors for client's retry (`pr#59378 <https://github.com/ceph/ceph/pull/59378>`_, Radoslaw Zarzynski)
* osd: avoid watcher remains after "rados watch" is interrupted (`pr#58845 <https://github.com/ceph/ceph/pull/58845>`_, weixinwei)
* osd: bring the missed fmt::formatter for snapid_t to address FTBFS (`pr#54175 <https://github.com/ceph/ceph/pull/54175>`_, Radosław Zarzyński)
* osd: CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE flag is passed from ECBackend (`pr#57620 <https://github.com/ceph/ceph/pull/57620>`_, Md Mahamudur Rahaman Sajib)
* osd: do not assert on fast shutdown timeout (`pr#55134 <https://github.com/ceph/ceph/pull/55134>`_, Igor Fedotov)
* osd: don't require RWEXCL lock for stat+write ops (`pr#54594 <https://github.com/ceph/ceph/pull/54594>`_, Alice Zhao)
* osd: ensure async recovery does not drop a pg below min_size (`pr#54549 <https://github.com/ceph/ceph/pull/54549>`_, Samuel Just)
* osd: fix for segmentation fault on OSD fast shutdown (`pr#57614 <https://github.com/ceph/ceph/pull/57614>`_, Md Mahamudur Rahaman Sajib)
* osd: fix use-after-move in build_incremental_map_msg() (`pr#54269 <https://github.com/ceph/ceph/pull/54269>`_, Ronen Friedman)
* osd: improve OSD robustness (`pr#54785 <https://github.com/ceph/ceph/pull/54785>`_, Igor Fedotov)
* osd: log the number of extents for sparse read (`pr#54605 <https://github.com/ceph/ceph/pull/54605>`_, Xiubo Li)
* osd: make _set_cache_sizes ratio aware of cache_kv_onode_ratio (`pr#55235 <https://github.com/ceph/ceph/pull/55235>`_, Raimund Sacherer)
* osd: Report health error if OSD public address is not within subnet (`pr#55698 <https://github.com/ceph/ceph/pull/55698>`_, Prashant D)
* override client features (`pr#58227 <https://github.com/ceph/ceph/pull/58227>`_, Patrick Donnelly)
* pybind/mgr/devicehealth: replace SMART data if exists for same DATETIME (`pr#54880 <https://github.com/ceph/ceph/pull/54880>`_, Patrick Donnelly)
* pybind/mgr/devicehealth: skip legacy objects that cannot be loaded (`pr#56480 <https://github.com/ceph/ceph/pull/56480>`_, Patrick Donnelly)
* pybind/mgr/mirroring: drop mon_host from peer_list (`pr#55238 <https://github.com/ceph/ceph/pull/55238>`_, Jos Collin)
* pybind/mgr/pg_autoscaler: Cut back osdmap.get_pools calls (`pr#54904 <https://github.com/ceph/ceph/pull/54904>`_, Kamoltat)
* pybind/mgr/volumes: log mutex locks to help debug deadlocks (`pr#53917 <https://github.com/ceph/ceph/pull/53917>`_, Kotresh HR)
* pybind/mgr: disable sqlite3/python autocommit (`pr#57199 <https://github.com/ceph/ceph/pull/57199>`_, Patrick Donnelly)
* pybind/mgr: reopen database handle on blocklist (`pr#52461 <https://github.com/ceph/ceph/pull/52461>`_, Patrick Donnelly)
* pybind/rbd: don't produce info on errors in aio_mirror_image_get_info() (`pr#54054 <https://github.com/ceph/ceph/pull/54054>`_, Ilya Dryomov)
* pybind/rbd: expose CLONE_FORMAT and FLATTEN image options (`pr#57308 <https://github.com/ceph/ceph/pull/57308>`_, Ilya Dryomov)
* python-common/drive_group: handle fields outside of 'spec' even when 'spec' is provided (`pr#55962 <https://github.com/ceph/ceph/pull/55962>`_, Adam King)
* python-common/drive_selection: fix limit with existing devices (`pr#56085 <https://github.com/ceph/ceph/pull/56085>`_, Adam King)
* python-common/drive_selection: lower log level of limit policy message (`pr#55961 <https://github.com/ceph/ceph/pull/55961>`_, Adam King)
* python-common: fix osdspec_affinity check (`pr#56084 <https://github.com/ceph/ceph/pull/56084>`_, Guillaume Abrioux)
* python-common: handle "anonymous_access: false" in to_json of Grafana spec (`pr#58756 <https://github.com/ceph/ceph/pull/58756>`_, Adam King)
* qa/cephadm: testing for extra daemon/container features (`pr#55958 <https://github.com/ceph/ceph/pull/55958>`_, Adam King)
* qa/cephfs: add mgr debugging (`pr#56417 <https://github.com/ceph/ceph/pull/56417>`_, Patrick Donnelly)
* qa/cephfs: add probabilistic ignorelist for pg_health (`pr#56667 <https://github.com/ceph/ceph/pull/56667>`_, Patrick Donnelly)
* qa/cephfs: CephFSTestCase.create_client() must keyring (`pr#56837 <https://github.com/ceph/ceph/pull/56837>`_, Rishabh Dave)
* qa/cephfs: fix build failure for mdtest project (`pr#53826 <https://github.com/ceph/ceph/pull/53826>`_, Rishabh Dave)
* qa/cephfs: fix ior project build failure (`pr#53824 <https://github.com/ceph/ceph/pull/53824>`_, Rishabh Dave)
* qa/cephfs: handle non-numeric values for json.loads() (`pr#54187 <https://github.com/ceph/ceph/pull/54187>`_, Rishabh Dave)
* qa/cephfs: ignorelist clog of MDS_UP_LESS_THAN_MAX (`pr#56404 <https://github.com/ceph/ceph/pull/56404>`_, Patrick Donnelly)
* qa/cephfs: no reliance on centos (`pr#59037 <https://github.com/ceph/ceph/pull/59037>`_, Venky Shankar)
* qa/cephfs: switch to python3 for centos stream 9 (`pr#53626 <https://github.com/ceph/ceph/pull/53626>`_, Xiubo Li)
* qa/distros: backport update from rhel 8.4 -> 8.6 (`pr#54902 <https://github.com/ceph/ceph/pull/54902>`_, David Galloway)
* qa/distros: replace centos 8 references with centos 9 in the rados suite (`pr#58520 <https://github.com/ceph/ceph/pull/58520>`_, Laura Flores)
* qa/orch: drop centos 8 and rhel 8.6 for orch suite tests (`pr#58769 <https://github.com/ceph/ceph/pull/58769>`_, Adam King, Laura Flores, Guillaume Abrioux, Casey Bodley)
* qa/rgw: adapt tests to centos 9 (`pr#58601 <https://github.com/ceph/ceph/pull/58601>`_, Mark Kogan, Casey Bodley, Ali Maredia, Yuval Lifshitz)
* qa/rgw: barbican uses branch stable/2023.1 (`pr#56818 <https://github.com/ceph/ceph/pull/56818>`_, Casey Bodley)
* qa/suites/fs/nfs: use standard health ignorelist (`pr#56393 <https://github.com/ceph/ceph/pull/56393>`_, Patrick Donnelly)
* qa/suites/fs: skip check-counters for iogen workload (`pr#58278 <https://github.com/ceph/ceph/pull/58278>`_, Ramana Raja)
* qa/suites/krbd: drop pre-single-major and move "layering only" coverage (`pr#57463 <https://github.com/ceph/ceph/pull/57463>`_, Ilya Dryomov)
* qa/suites/krbd: stress test for recovering from watch errors for -o exclusive (`pr#58855 <https://github.com/ceph/ceph/pull/58855>`_, Ilya Dryomov)
* qa/suites/rados/singleton: add POOL_APP_NOT_ENABLED to ignorelist (`pr#57488 <https://github.com/ceph/ceph/pull/57488>`_, Laura Flores)
* qa/suites/rbd/iscsi: enable all supported container hosts (`pr#60087 <https://github.com/ceph/ceph/pull/60087>`_, Ilya Dryomov)
* qa/suites/rbd: add test to check rbd_support module recovery (`pr#54292 <https://github.com/ceph/ceph/pull/54292>`_, Ramana Raja)
* qa/suites/rbd: override extra_system_packages directly on install task (`pr#57764 <https://github.com/ceph/ceph/pull/57764>`_, Ilya Dryomov)
* qa/suites/upgrade/quincy-p2p: run librbd python API tests from quincy tip (`pr#55554 <https://github.com/ceph/ceph/pull/55554>`_, Yuri Weinstein)
* qa/suites: add "mon down" log variations to ignorelist (`pr#58762 <https://github.com/ceph/ceph/pull/58762>`_, Laura Flores)
* qa/suites: drop --show-reachable=yes from fs:valgrind tests (`pr#59067 <https://github.com/ceph/ceph/pull/59067>`_, Jos Collin)
* qa/tasks/ceph_manager.py: Rewrite test_pool_min_size (`pr#55882 <https://github.com/ceph/ceph/pull/55882>`_, Kamoltat)
* qa/tasks/cephfs/test_misc: switch duration to timeout (`pr#55745 <https://github.com/ceph/ceph/pull/55745>`_, Xiubo Li)
* qa/tasks/qemu: Fix OS version comparison (`pr#58169 <https://github.com/ceph/ceph/pull/58169>`_, Zack Cerza)
* qa/test_nfs: fix test failure when cluster does not exist (`pr#56753 <https://github.com/ceph/ceph/pull/56753>`_, John Mulligan)
* qa/tests: added client-upgrade-quincy-squid tests (`pr#58445 <https://github.com/ceph/ceph/pull/58445>`_, Yuri Weinstein)
* qa/workunits/rados: enable crb and install generic package for c9 (`pr#59330 <https://github.com/ceph/ceph/pull/59330>`_, Laura Flores)
* qa/workunits/rbd/cli_generic.sh: narrow race window when checking that rbd_support module command fails after blocklisting the module's client (`pr#54770 <https://github.com/ceph/ceph/pull/54770>`_, Ramana Raja)
* qa/workunits/rbd: avoid caching effects in luks-encryption.sh (`pr#58852 <https://github.com/ceph/ceph/pull/58852>`_, Ilya Dryomov, Or Ozeri)
* qa/workunits: fix test_dashboard_e2e.sh: no spec files found (`pr#53857 <https://github.com/ceph/ceph/pull/53857>`_, Nizamudeen A)
* qa: account for rbd_trash object in krbd_data_pool.sh + related ceph{,adm} task fixes (`pr#58539 <https://github.com/ceph/ceph/pull/58539>`_, Ilya Dryomov)
* qa: add a YAML to ignore MGR_DOWN warning (`pr#57564 <https://github.com/ceph/ceph/pull/57564>`_, Dhairya Parmar)
* qa: add diff-continuous and compare-mirror-image tests to rbd and krbd suites respectively (`pr#55929 <https://github.com/ceph/ceph/pull/55929>`_, Ramana Raja)
* qa: Add tests to validate synced images on rbd-mirror (`pr#55763 <https://github.com/ceph/ceph/pull/55763>`_, Ilya Dryomov, Ramana Raja)
* qa: adjust expected io_opt in krbd_discard_granularity.t (`pr#59230 <https://github.com/ceph/ceph/pull/59230>`_, Ilya Dryomov)
* qa: assign file system affinity for replaced MDS (`issue#61764 <http://tracker.ceph.com/issues/61764>`_, `pr#54038 <https://github.com/ceph/ceph/pull/54038>`_, Venky Shankar)
* qa: barbican: restrict python packages with upper-constraints (`pr#59325 <https://github.com/ceph/ceph/pull/59325>`_, Tobias Urdin)
* qa: bump up scrub status command timeout (`pr#55916 <https://github.com/ceph/ceph/pull/55916>`_, Milind Changire)
* qa: cleanup snapshots before subvolume delete (`pr#58333 <https://github.com/ceph/ceph/pull/58333>`_, Milind Changire)
* qa: correct usage of DEBUGFS_META_DIR in dedent (`pr#56166 <https://github.com/ceph/ceph/pull/56166>`_, Venky Shankar)
* qa: fix error reporting string in assert_cluster_log (`pr#55392 <https://github.com/ceph/ceph/pull/55392>`_, Dhairya Parmar)
* qa: Fix fs/full suite (`pr#55828 <https://github.com/ceph/ceph/pull/55828>`_, Kotresh HR)
* qa: fix krbd_msgr_segments and krbd_rxbounce failing on 8.stream (`pr#57029 <https://github.com/ceph/ceph/pull/57029>`_, Ilya Dryomov)
* qa: fix rank_asok() to handle errors from asok commands (`pr#55301 <https://github.com/ceph/ceph/pull/55301>`_, Neeraj Pratap Singh)
* qa: ignore container checkpoint/restore related selinux denials for c… (`issue#67119 <http://tracker.ceph.com/issues/67119>`_, `issue#66640 <http://tracker.ceph.com/issues/66640>`_, `pr#58807 <https://github.com/ceph/ceph/pull/58807>`_, Venky Shankar)
* qa: increase the http postBuffer size and disable sslVerify (`pr#53629 <https://github.com/ceph/ceph/pull/53629>`_, Xiubo Li)
* qa: lengthen shutdown timeout for thrashed MDS (`pr#53554 <https://github.com/ceph/ceph/pull/53554>`_, Patrick Donnelly)
* qa: move nfs (mgr/nfs) related tests to fs suite (`pr#53907 <https://github.com/ceph/ceph/pull/53907>`_, Dhairya Parmar, Venky Shankar)
* qa: remove error string checks and check w/ return value (`pr#55944 <https://github.com/ceph/ceph/pull/55944>`_, Venky Shankar)
* qa: remove vstart runner from radosgw_admin task (`pr#55098 <https://github.com/ceph/ceph/pull/55098>`_, Ali Maredia)
* qa: run kernel_untar_build with newer tarball (`pr#54712 <https://github.com/ceph/ceph/pull/54712>`_, Milind Changire)
* qa: set mds config with `config set` for a particular test (`issue#57087 <http://tracker.ceph.com/issues/57087>`_, `pr#56168 <https://github.com/ceph/ceph/pull/56168>`_, Venky Shankar)
* qa: unmount clients before damaging the fs (`pr#57526 <https://github.com/ceph/ceph/pull/57526>`_, Patrick Donnelly)
* qa: Wait for purge to complete (`pr#53911 <https://github.com/ceph/ceph/pull/53911>`_, Kotresh HR)
* rados: Set snappy as default value in ms_osd_compression_algorithm (`pr#57406 <https://github.com/ceph/ceph/pull/57406>`_, shreyanshjain7174)
* RadosGW API: incorrect bucket quota in response to HEAD /{bucket}/?usage (`pr#53438 <https://github.com/ceph/ceph/pull/53438>`_, shreyanshjain7174)
* radosgw-admin: don't crash on --placement-id without --storage-class (`pr#53473 <https://github.com/ceph/ceph/pull/53473>`_, Casey Bodley)
* radosgw-admin: fix segfault on pipe modify without source/dest zone specified (`pr#51257 <https://github.com/ceph/ceph/pull/51257>`_, caisan)
* rbd-mirror: clean up stale pool replayers and callouts better (`pr#57305 <https://github.com/ceph/ceph/pull/57305>`_, Ilya Dryomov)
* rbd-mirror: use correct ioctx for namespace (`pr#59774 <https://github.com/ceph/ceph/pull/59774>`_, N Balachandran)
* rbd-nbd: fix resize of images mapped using netlink (`pr#55317 <https://github.com/ceph/ceph/pull/55317>`_, Ramana Raja)
* rbd-nbd: fix stuck with disable request (`pr#54255 <https://github.com/ceph/ceph/pull/54255>`_, Prasanna Kumar Kalever)
* rbd: "rbd bench" always writes the same byte (`pr#59500 <https://github.com/ceph/ceph/pull/59500>`_, Ilya Dryomov)
* rbd: amend "rbd {group,} rename" and "rbd mirror pool" command descriptions (`pr#59600 <https://github.com/ceph/ceph/pull/59600>`_, Ilya Dryomov)
* Revert "exporter: user only counter dump/schema commands for extacting counters" (`pr#54169 <https://github.com/ceph/ceph/pull/54169>`_, Casey Bodley)
* Revert "quincy: ceph_fs.h: add separate owner\_{u,g}id fields" (`pr#54108 <https://github.com/ceph/ceph/pull/54108>`_, Venky Shankar)
* RGW - Get quota on OPs with a bucket (`pr#52935 <https://github.com/ceph/ceph/pull/52935>`_, Daniel Gryniewicz)
* rgw : fix add initialization for RGWGC::process() (`pr#59338 <https://github.com/ceph/ceph/pull/59338>`_, caolei)
* rgw/admin/notifications: support admin operations on topics with tenants (`pr#59322 <https://github.com/ceph/ceph/pull/59322>`_, Yuval Lifshitz)
* rgw/amqp: store CA location string in connection object (`pr#54170 <https://github.com/ceph/ceph/pull/54170>`_, Yuval Lifshitz)
* rgw/auth/s3: validate x-amz-content-sha256 for empty payloads (`pr#59359 <https://github.com/ceph/ceph/pull/59359>`_, Casey Bodley)
* rgw/auth: Add service token support for Keystone auth (`pr#54445 <https://github.com/ceph/ceph/pull/54445>`_, Tobias Urdin)
* rgw/auth: Fix the return code returned by AuthStrategy (`pr#54795 <https://github.com/ceph/ceph/pull/54795>`_, Pritha Srivastava)
* rgw/auth: ignoring signatures for HTTP OPTIONS calls (`pr#60458 <https://github.com/ceph/ceph/pull/60458>`_, Tobias Urdin)
* rgw/beast: Enable SSL session-id reuse speedup mechanism (`pr#56119 <https://github.com/ceph/ceph/pull/56119>`_, Mark Kogan)
* rgw/crypt: apply rgw_crypt_default_encryption_key by default (`pr#52795 <https://github.com/ceph/ceph/pull/52795>`_, Casey Bodley)
* rgw/iam: admin/system users ignore iam policy parsing errors (`pr#54842 <https://github.com/ceph/ceph/pull/54842>`_, Casey Bodley)
* rgw/kafka/amqp: fix race conditionn in async completion handlers (`pr#54737 <https://github.com/ceph/ceph/pull/54737>`_, Yuval Lifshitz)
* rgw/kafka: remove potential race condition between creation and deletion of endpoint (`pr#51797 <https://github.com/ceph/ceph/pull/51797>`_, Yuval Lifshitz)
* rgw/kafka: set message timeout to 5 seconds (`pr#56163 <https://github.com/ceph/ceph/pull/56163>`_, Yuval Lifshitz)
* rgw/keystone: EC2Engine uses reject() for ERR_SIGNATURE_NO_MATCH (`pr#53763 <https://github.com/ceph/ceph/pull/53763>`_, Casey Bodley)
* rgw/keystone: use secret key from EC2 for sigv4 streaming mode (`pr#57899 <https://github.com/ceph/ceph/pull/57899>`_, Casey Bodley)
* rgw/lua: add lib64 to the package search path (`pr#59342 <https://github.com/ceph/ceph/pull/59342>`_, Yuval Lifshitz)
* rgw/lua: fix CopyFrom crash (`pr#59336 <https://github.com/ceph/ceph/pull/59336>`_, Yuval Lifshitz)
* rgw/multisite: fix sync_error_trim command (`pr#59347 <https://github.com/ceph/ceph/pull/59347>`_, Shilpa Jagannath)
* rgw/notification: Kafka persistent notifications not retried and removed even when the broker is down (`pr#56145 <https://github.com/ceph/ceph/pull/56145>`_, kchheda3)
* rgw/notification: remove non x-amz-meta-\* attributes from bucket notifications (`pr#53374 <https://github.com/ceph/ceph/pull/53374>`_, Juan Zhu)
* rgw/notifications/test: fix rabbitmq and kafka issues in centos9 (`pr#58313 <https://github.com/ceph/ceph/pull/58313>`_, Yuval Lifshitz)
* rgw/notifications: cleanup all coroutines after sending the notification (`pr#59353 <https://github.com/ceph/ceph/pull/59353>`_, Yuval Lifshitz)
* rgw/putobj: RadosWriter uses part head object for multipart parts (`pr#55622 <https://github.com/ceph/ceph/pull/55622>`_, Casey Bodley)
* rgw/rest: fix url decode of post params for iam/sts/sns (`pr#55357 <https://github.com/ceph/ceph/pull/55357>`_, Casey Bodley)
* rgw/rgw-gap-list: refactoring and adding more error checking (`pr#59320 <https://github.com/ceph/ceph/pull/59320>`_, Michael J. Kidd)
* rgw/rgw-orphan-list: refactor and add more checks to the tool (`pr#59321 <https://github.com/ceph/ceph/pull/59321>`_, Michael J. Kidd)
* rgw/s3: DeleteObjects response uses correct delete_marker flag (`pr#54165 <https://github.com/ceph/ceph/pull/54165>`_, Casey Bodley)
* rgw/s3: ListObjectsV2 returns correct object owners (`pr#54162 <https://github.com/ceph/ceph/pull/54162>`_, Casey Bodley)
* rgw/sts: AssumeRole no longer writes to user metadata (`pr#52049 <https://github.com/ceph/ceph/pull/52049>`_, Casey Bodley)
* rgw/sts: changing identity to boost::none, when role policy (`pr#59345 <https://github.com/ceph/ceph/pull/59345>`_, Pritha Srivastava)
* rgw/sts: modify max_session_duration using update role REST API/ radosgw-admin command (`pr#48082 <https://github.com/ceph/ceph/pull/48082>`_, Pritha Srivastava)
* RGW/STS: when generating keys, take the trailing null character into account (`pr#54128 <https://github.com/ceph/ceph/pull/54128>`_, Oguzhan Ozmen)
* rgw/swift: preserve dashes/underscores in swift user metadata names (`pr#56616 <https://github.com/ceph/ceph/pull/56616>`_, Juan Zhu, Ali Maredia)
* rgw: 'bucket check' deletes index of multipart meta when its pending_map is nonempty (`pr#54017 <https://github.com/ceph/ceph/pull/54017>`_, Huber-ming)
* rgw: add crypt attrs for iam policy to PostObj and Init/CompleteMultipart (`pr#59344 <https://github.com/ceph/ceph/pull/59344>`_, Casey Bodley)
* rgw: add headers to guide cache update in 304 response (`pr#55095 <https://github.com/ceph/ceph/pull/55095>`_, Casey Bodley, Ilsoo Byun)
* rgw: Add missing empty checks to the split string in is_string_in_set() (`pr#56348 <https://github.com/ceph/ceph/pull/56348>`_, Matt Benjamin)
* rgw: add versioning info to radosgw-admin bucket stats output (`pr#54190 <https://github.com/ceph/ceph/pull/54190>`_, J. Eric Ivancich, Cory Snyder)
* rgw: address crash and race in RGWIndexCompletionManager (`pr#50538 <https://github.com/ceph/ceph/pull/50538>`_, J. Eric Ivancich)
* RGW: allow user disabling presigned urls in rgw configuration (`pr#56447 <https://github.com/ceph/ceph/pull/56447>`_, Marc Singer)
* rgw: avoid use-after-move in RGWDataSyncSingleEntryCR ctor (`pr#59319 <https://github.com/ceph/ceph/pull/59319>`_, Casey Bodley)
* rgw: beast frontend checks for local_endpoint() errors (`pr#54166 <https://github.com/ceph/ceph/pull/54166>`_, Casey Bodley)
* rgw: catches nobjects_begin() exceptions (`pr#59360 <https://github.com/ceph/ceph/pull/59360>`_, lichaochao)
* rgw: cmake configure error on fedora-37/rawhide (`pr#59313 <https://github.com/ceph/ceph/pull/59313>`_, Kaleb S. KEITHLEY)
* rgw: CopyObject works with x-amz-copy-source-if-\* headers (`pr#50519 <https://github.com/ceph/ceph/pull/50519>`_, Wang Hao)
* rgw: d3n: fix valgrind reported leak related to libaio worker threads (`pr#54851 <https://github.com/ceph/ceph/pull/54851>`_, Mark Kogan)
* rgw: disable RGWDataChangesLog::add_entry() when log_data is off (`pr#59314 <https://github.com/ceph/ceph/pull/59314>`_, Casey Bodley)
* rgw: do not copy olh attributes in versioning suspended bucket (`pr#55607 <https://github.com/ceph/ceph/pull/55607>`_, Juan Zhu)
* rgw: Drain async_processor request queue during shutdown (`pr#53471 <https://github.com/ceph/ceph/pull/53471>`_, Soumya Koduri)
* rgw: Erase old storage class attr when the object is rewrited using r… (`pr#50520 <https://github.com/ceph/ceph/pull/50520>`_, zhiming zhang)
* rgw: Fix Browser POST content-length-range min value (`pr#52937 <https://github.com/ceph/ceph/pull/52937>`_, Robin H. Johnson)
* rgw: fix issue with concurrent versioned deletes leaving behind olh entries (`pr#59357 <https://github.com/ceph/ceph/pull/59357>`_, Cory Snyder)
* rgw: fix ListOpenIDConnectProviders XML format (`pr#57131 <https://github.com/ceph/ceph/pull/57131>`_, caolei)
* rgw: fix multipart upload object leaks due to re-upload (`pr#51976 <https://github.com/ceph/ceph/pull/51976>`_, J. Eric Ivancich, Yixin Jin, Matt Benjamin, Daniel Gryniewicz)
* rgw: fix rgw cache invalidation after unregister_watch() error (`pr#54015 <https://github.com/ceph/ceph/pull/54015>`_, lichaochao)
* rgw: Get canonical storage class when storage class is empty in (`pr#59317 <https://github.com/ceph/ceph/pull/59317>`_, zhiming zhang)
* rgw: handle old clients with transfer-encoding: chunked (`pr#57133 <https://github.com/ceph/ceph/pull/57133>`_, Marcus Watts)
* rgw: invalidate and retry keystone admin token (`pr#59076 <https://github.com/ceph/ceph/pull/59076>`_, Tobias Urdin)
* rgw: make incomplete multipart upload part of bucket check efficient (`pr#57405 <https://github.com/ceph/ceph/pull/57405>`_, J. Eric Ivancich)
* rgw: modify string match_wildcards with fnmatch (`pr#57907 <https://github.com/ceph/ceph/pull/57907>`_, zhipeng li, Adam Emerson)
* rgw: multisite data log flag not used (`pr#52054 <https://github.com/ceph/ceph/pull/52054>`_, J. Eric Ivancich)
* rgw: object lock avoids 32-bit truncation of RetainUntilDate (`pr#54675 <https://github.com/ceph/ceph/pull/54675>`_, Casey Bodley)
* rgw: remove potentially conficting definition of dout_subsys (`pr#53462 <https://github.com/ceph/ceph/pull/53462>`_, J. Eric Ivancich)
* rgw: RGWSI_SysObj_Cache::remove() invalidates after successful delete (`pr#55718 <https://github.com/ceph/ceph/pull/55718>`_, Casey Bodley)
* rgw: s3 object lock avoids overflow in retention date (`pr#52606 <https://github.com/ceph/ceph/pull/52606>`_, Casey Bodley)
* rgw: set requestPayment in slave zone (`pr#57149 <https://github.com/ceph/ceph/pull/57149>`_, Huber-ming)
* rgw: SignatureDoesNotMatch for certain RGW Admin Ops endpoints w/v4 auth (`pr#54792 <https://github.com/ceph/ceph/pull/54792>`_, David.Hall)
* RGW: Solving the issue of not populating etag in Multipart upload result (`pr#51446 <https://github.com/ceph/ceph/pull/51446>`_, Ali Masarwa)
* rgw: swift: tempurl fixes for ceph (`pr#59355 <https://github.com/ceph/ceph/pull/59355>`_, Casey Bodley, Adam Emerson, Marcus Watts)
* rgw: Update "CEPH_RGW_DIR_SUGGEST_LOG_OP" for remove entries (`pr#50539 <https://github.com/ceph/ceph/pull/50539>`_, Soumya Koduri)
* rgw: update options yaml file so LDAP uri isn't an invalid example (`pr#56722 <https://github.com/ceph/ceph/pull/56722>`_, J. Eric Ivancich)
* rgw: Use STANDARD storage class in objects appending operation when the (`pr#59316 <https://github.com/ceph/ceph/pull/59316>`_, zhiming zhang)
* rgw: use unique_ptr for flat_map emplace in BucketTrimWatche (`pr#52995 <https://github.com/ceph/ceph/pull/52995>`_, Vedansh Bhartia)
* rgw: when there are a large number of multiparts, the unorder list result may miss objects (`pr#59337 <https://github.com/ceph/ceph/pull/59337>`_, J. Eric Ivancich)
* rgwfile: fix lock_guard decl (`pr#59350 <https://github.com/ceph/ceph/pull/59350>`_, Matt Benjamin)
* rgwlc: fix compat-decoding of cls_rgw_lc_get_entry_ret (`pr#59312 <https://github.com/ceph/ceph/pull/59312>`_, Matt Benjamin)
* rgwlc: permit lifecycle to reduce data conditionally in archive zone (`pr#54873 <https://github.com/ceph/ceph/pull/54873>`_, Matt Benjamin)
* run-make-check: use get_processors in run-make-check script (`pr#58871 <https://github.com/ceph/ceph/pull/58871>`_, John Mulligan)
* src/ceph-volume/ceph_volume/devices/lvm/listing.py : lvm list filters with vg name (`pr#58999 <https://github.com/ceph/ceph/pull/58999>`_, Pierre Lemay)
* src/common/options: Correct typo in rgw.yaml.in (`pr#55446 <https://github.com/ceph/ceph/pull/55446>`_, Anthony D'Atri)
* src/mon/Monitor: Fix set_elector_disallowed_leaders (`pr#54004 <https://github.com/ceph/ceph/pull/54004>`_, Kamoltat)
* src/mount: kernel mount command returning misleading error message (`pr#55299 <https://github.com/ceph/ceph/pull/55299>`_, Neeraj Pratap Singh)
* test/cls_lock: expired lock before unlock and start check (`pr#59272 <https://github.com/ceph/ceph/pull/59272>`_, Nitzan Mordechai)
* test/lazy-omap-stats: Convert to boost::regex (`pr#59523 <https://github.com/ceph/ceph/pull/59523>`_, Brad Hubbard)
* test/librbd: clean up unused TEST_COOKIE variable (`pr#58548 <https://github.com/ceph/ceph/pull/58548>`_, Rongqi Sun)
* test/pybind: replace nose with pytest (`pr#55060 <https://github.com/ceph/ceph/pull/55060>`_, Casey Bodley)
* test/rgw/notifications: fix kafka consumer shutdown issue (`pr#59340 <https://github.com/ceph/ceph/pull/59340>`_, Yuval Lifshitz)
* test/rgw: increase timeouts in unittest_rgw_dmclock_scheduler (`pr#55789 <https://github.com/ceph/ceph/pull/55789>`_, Casey Bodley)
* test/store_test: enforce sync compactions for spillover tests (`pr#59532 <https://github.com/ceph/ceph/pull/59532>`_, Igor Fedotov)
* test/store_test: fix deferred writing test cases (`pr#55779 <https://github.com/ceph/ceph/pull/55779>`_, Igor Fedotov)
* test/store_test: fix DeferredWrite test when prefer_deferred_size=0 (`pr#56201 <https://github.com/ceph/ceph/pull/56201>`_, Igor Fedotov)
* test/store_test: get rid off assert_death (`pr#55775 <https://github.com/ceph/ceph/pull/55775>`_, Igor Fedotov)
* test/store_test: refactor spillover tests (`pr#55216 <https://github.com/ceph/ceph/pull/55216>`_, Igor Fedotov)
* test: Create ParallelPGMapper object before start threadpool (`pr#58921 <https://github.com/ceph/ceph/pull/58921>`_, Mohit Agrawal)
* Test: osd-recovery-space.sh extends the wait time for "recovery toofull" (`pr#59042 <https://github.com/ceph/ceph/pull/59042>`_, Nitzan Mordechai)
* tools/ceph_objectstore_tool: action_on_all_objects_in_pg to skip pgmeta (`pr#54692 <https://github.com/ceph/ceph/pull/54692>`_, Matan Breizman)
* tools/ceph_objectstore_tool: Support get/set/superblock (`pr#55014 <https://github.com/ceph/ceph/pull/55014>`_, Matan Breizman)
* Tools/rados: Improve Error Messaging for Object Name Resolution (`pr#55598 <https://github.com/ceph/ceph/pull/55598>`_, Nitzan Mordechai)
* tools/rbd: make 'children' command support --image-id (`pr#55618 <https://github.com/ceph/ceph/pull/55618>`_, Mykola Golub)
* win32_deps_build.sh: change Boost URL (`pr#55085 <https://github.com/ceph/ceph/pull/55085>`_, Lucian Petrut)

v17.2.7 Quincy
==============

This is the seventh backport release in the Quincy series. We recommend
that all users update to this release.

Notable Changes
---------------

* `ceph mgr dump` command now displays the name of the Manager module that
  registered a RADOS client in the `name` field added to elements of the
  `active_clients` array. Previously, only the address of a module's RADOS
  client was shown in the `active_clients` array.

* mClock Scheduler: The mClock scheduler (default scheduler in Quincy) has
  undergone significant usability and design improvements to address the slow
  backfill issue. Some important changes are:

  * The 'balanced' profile is set as the default mClock profile because it
    represents a compromise between prioritizing client IO or recovery IO. Users
    can then choose either the 'high_client_ops' profile to prioritize client IO
    or the 'high_recovery_ops' profile to prioritize recovery IO.

  * QoS parameters including reservation and limit are now specified in terms
    of a fraction (range: 0.0 to 1.0) of the OSD's IOPS capacity.

  * The cost parameters (osd_mclock_cost_per_io_usec_* and
    osd_mclock_cost_per_byte_usec_*) have been removed. The cost of an operation
    is now determined using the random IOPS and maximum sequential bandwidth
    capability of the OSD's underlying device.

  * Degraded object recovery is given higher priority when compared to misplaced
    object recovery because degraded objects present a data safety issue not
    present with objects that are merely misplaced. Therefore, backfilling
    operations with the 'balanced' and 'high_client_ops' mClock profiles may
    progress slower than what was seen with the 'WeightedPriorityQueue' (WPQ)
    scheduler.

  * The QoS allocations in all mClock profiles are optimized based on the above
    fixes and enhancements.

  * For more detailed information see:
    https://docs.ceph.com/en/quincy/rados/configuration/mclock-config-ref/

* RGW: S3 multipart uploads using Server-Side Encryption now replicate
  correctly in multi-site. Previously, the replicas of such objects were
  corrupted on decryption.  A new tool, ``radosgw-admin bucket resync encrypted
  multipart``, can be used to identify these original multipart uploads. The
  ``LastModified`` timestamp of any identified object is incremented by 1
  nanosecond to cause peer zones to replicate it again.  For multi-site
  deployments that make any use of Server-Side Encryption, we recommended
  running this command against every bucket in every zone after all zones have
  upgraded.

* CephFS: MDS evicts clients which are not advancing their request tids which
  causes a large buildup of session metadata resulting in the MDS going
  read-only due to the RADOS operation exceeding the size threshold.
  `mds_session_metadata_threshold` config controls the maximum size that a
  (encoded) session metadata can grow.

* CephFS: After recovering a Ceph File System post following the disaster
  recovery procedure, the recovered files under `lost+found` directory can now
  be deleted.

* Dashboard: There is a new Dashboard page with an improved layout. Active alerts
  and some important charts are now displayed inside cards. This new dashboard can
  be disabled and the older layout brought back by setting ``ceph dashboard feature disable dashboard``.

Changelog
---------

* .github: Clarify checklist details (`pr#54131 <https://github.com/ceph/ceph/pull/54131>`_, Anthony D'Atri)
* .github: Give folks 30 seconds to fill out the checklist (`pr#51944 <https://github.com/ceph/ceph/pull/51944>`_, David Galloway)
* [CVE-2023-43040] rgw: Fix bucket validation against POST policies (`pr#53757 <https://github.com/ceph/ceph/pull/53757>`_, Joshua Baergen)
* backport commit 70425c7 -- client/fuse: set max_idle_threads to the correct value (critical, ceph-fuse with libfuse3 is nearly useless without it) (`pr#50668 <https://github.com/ceph/ceph/pull/50668>`_, Zhansong Gao)
* blk/kernel: Add O_EXCL for block devices (`pr#53566 <https://github.com/ceph/ceph/pull/53566>`_, Adam Kupczyk)
* blk/kernel: Fix error code mapping in KernelDevice::read (`pr#49984 <https://github.com/ceph/ceph/pull/49984>`_, Joshua Baergen)
* blk/KernelDevice: Modify the rotational and discard check log message (`pr#50323 <https://github.com/ceph/ceph/pull/50323>`_, Vikhyat Umrao)
* Bluestore: fix bluestore collection_list latency perf counter (`pr#52951 <https://github.com/ceph/ceph/pull/52951>`_, Wangwenjuan)
* build: make it possible to build w/o ceph-mgr (`pr#54132 <https://github.com/ceph/ceph/pull/54132>`_, J. Eric Ivancich)
* build: Remove ceph-libboost\* packages in install-deps (`pr#52564 <https://github.com/ceph/ceph/pull/52564>`_, Nizamudeen A, Adam Emerson)
* ceph-volume/cephadm: support lv devices in inventory (`pr#53287 <https://github.com/ceph/ceph/pull/53287>`_, Guillaume Abrioux)
* ceph-volume: add --osd-id option to raw prepare (`pr#52929 <https://github.com/ceph/ceph/pull/52929>`_, Guillaume Abrioux)
* ceph-volume: fix a bug in `get_lvm_fast_allocs()` (batch) (`pr#52062 <https://github.com/ceph/ceph/pull/52062>`_, Guillaume Abrioux)
* ceph-volume: fix batch refactor issue (`pr#51206 <https://github.com/ceph/ceph/pull/51206>`_, Guillaume Abrioux)
* ceph-volume: fix drive-group issue that expects the batch_args to be a string (`pr#51210 <https://github.com/ceph/ceph/pull/51210>`_, Mohan Sharma)
* ceph-volume: fix inventory with device arg (`pr#48125 <https://github.com/ceph/ceph/pull/48125>`_, Guillaume Abrioux)
* ceph-volume: fix issue with fast device allocs when there are multiple PVs per VG (`pr#50879 <https://github.com/ceph/ceph/pull/50879>`_, Cory Snyder)
* ceph-volume: fix mpath device support (`pr#53540 <https://github.com/ceph/ceph/pull/53540>`_, Guillaume Abrioux)
* ceph-volume: fix raw list for lvm devices (`pr#52620 <https://github.com/ceph/ceph/pull/52620>`_, Guillaume Abrioux)
* ceph-volume: quick fix in zap.py (`pr#51195 <https://github.com/ceph/ceph/pull/51195>`_, Guillaume Abrioux)
* ceph-volume: set lvm membership for mpath type devices (`pr#52079 <https://github.com/ceph/ceph/pull/52079>`_, Guillaume Abrioux)
* ceph-volume: update the OS before deploying Ceph (quincy) (`pr#50995 <https://github.com/ceph/ceph/pull/50995>`_, Guillaume Abrioux)
* ceph: allow xlock state to be LOCK_PREXLOCK when putting it (`pr#53663 <https://github.com/ceph/ceph/pull/53663>`_, Xiubo Li)
* ceph_volume: support encrypted volumes for lvm new-db/new-wal/migrate commands (`pr#52874 <https://github.com/ceph/ceph/pull/52874>`_, Igor Fedotov)
* cephadm: eliminate duplication of sections (`pr#51432 <https://github.com/ceph/ceph/pull/51432>`_, Rongqi Sun)
* cephadm: fix call timeout argument (`pr#52909 <https://github.com/ceph/ceph/pull/52909>`_, John Mulligan)
* cephadm: handle exceptions applying extra services during bootstrap (`pr#50904 <https://github.com/ceph/ceph/pull/50904>`_, Adam King)
* cephadm: mount host /etc/hosts for daemon containers in podman deployments (`pr#50902 <https://github.com/ceph/ceph/pull/50902>`_, Adam King, Ilya Dryomov)
* cephadm: reschedule haproxy from an offline host (`pr#51216 <https://github.com/ceph/ceph/pull/51216>`_, Michael Fritch)
* cephadm: set --ulimit nofiles with Docker (`pr#50890 <https://github.com/ceph/ceph/pull/50890>`_, Michal Nasiadka)
* cephadm: Split multicast interface and unicast_ip in keepalived.conf (`pr#53098 <https://github.com/ceph/ceph/pull/53098>`_, Luis Domingues)
* cephadm: using ip instead of short hostname for prometheus urls (`pr#50905 <https://github.com/ceph/ceph/pull/50905>`_, Redouane Kachach)
* cephfs-journal-tool: disambiguate usage of all keyword (in tool help) (`pr#53285 <https://github.com/ceph/ceph/pull/53285>`_, Manish M Yathnalli)
* cephfs-mirror: do not run concurrent C_RestartMirroring context (`issue#62072 <http://tracker.ceph.com/issues/62072>`_, `pr#53639 <https://github.com/ceph/ceph/pull/53639>`_, Venky Shankar)
* cephfs-top: check the minimum compatible python version (`pr#51354 <https://github.com/ceph/ceph/pull/51354>`_, Jos Collin)
* cephfs-top: dump values to stdout and -d [--delay] option fix (`pr#50717 <https://github.com/ceph/ceph/pull/50717>`_, Jos Collin, Neeraj Pratap Singh, wangxinyu, Rishabh Dave)
* cephfs-top: Handle `METRIC_TYPE_NONE` fields for sorting (`pr#50595 <https://github.com/ceph/ceph/pull/50595>`_, Neeraj Pratap Singh)
* cephfs-top: include the missing fields in --dump output (`pr#53454 <https://github.com/ceph/ceph/pull/53454>`_, Jos Collin)
* cephfs-top: navigate to home screen when no fs (`pr#50731 <https://github.com/ceph/ceph/pull/50731>`_, Jos Collin)
* cephfs-top: Some fixes in `choose_field()` for sorting (`pr#50365 <https://github.com/ceph/ceph/pull/50365>`_, Neeraj Pratap Singh)
* cephfs_mirror: correctly set top level dir permissions (`pr#50528 <https://github.com/ceph/ceph/pull/50528>`_, Milind Changire)
* client: clear the suid/sgid in fallocate path (`pr#50989 <https://github.com/ceph/ceph/pull/50989>`_, Lucian Petrut, Xiubo Li)
* client: do not send metrics until the MDS rank is ready (`pr#52502 <https://github.com/ceph/ceph/pull/52502>`_, Xiubo Li)
* client: force sending cap revoke ack always (`pr#52508 <https://github.com/ceph/ceph/pull/52508>`_, Xiubo Li)
* client: issue a cap release immediately if no cap exists (`pr#52851 <https://github.com/ceph/ceph/pull/52851>`_, Xiubo Li)
* client: move the Inode to new auth mds session when changing auth cap (`pr#53664 <https://github.com/ceph/ceph/pull/53664>`_, Xiubo Li)
* client: only wait for write MDS OPs when unmounting (`pr#52303 <https://github.com/ceph/ceph/pull/52303>`_, Xiubo Li)
* client: trigger to flush the buffer when making snapshot (`pr#52498 <https://github.com/ceph/ceph/pull/52498>`_, Xiubo Li)
* client: use deep-copy when setting permission during make_request (`pr#51486 <https://github.com/ceph/ceph/pull/51486>`_, Mer Xuanyi)
* client: wait rename to finish (`pr#52503 <https://github.com/ceph/ceph/pull/52503>`_, Xiubo Li)
* common: avoid redefining clock type on Windows (`pr#50573 <https://github.com/ceph/ceph/pull/50573>`_, Lucian Petrut)
* Consider setting "bulk" autoscale pool flag when automatically creating a data pool for CephFS (`pr#52902 <https://github.com/ceph/ceph/pull/52902>`_, Leonid Usov)
* debian: install cephfs-mirror systemd unit files and man page (`pr#52074 <https://github.com/ceph/ceph/pull/52074>`_, Jos Collin)
* doc,test: clean up crush rule min/max_size leftovers (`pr#52169 <https://github.com/ceph/ceph/pull/52169>`_, Ilya Dryomov)
* doc/architecture.rst - edit a sentence (`pr#53373 <https://github.com/ceph/ceph/pull/53373>`_, Zac Dover)
* doc/architecture.rst - edit up to "Cluster Map" (`pr#53367 <https://github.com/ceph/ceph/pull/53367>`_, Zac Dover)
* doc/architecture: "Edit HA Auth" (`pr#53620 <https://github.com/ceph/ceph/pull/53620>`_, Zac Dover)
* doc/architecture: "Edit HA Auth" (one of several) (`pr#53586 <https://github.com/ceph/ceph/pull/53586>`_, Zac Dover)
* doc/architecture: "Edit HA Auth" (one of several) (`pr#53492 <https://github.com/ceph/ceph/pull/53492>`_, Zac Dover)
* doc/architecture: edit "Calculating PG IDs" (`pr#53749 <https://github.com/ceph/ceph/pull/53749>`_, Zac Dover)
* doc/architecture: edit "Cluster Map" (`pr#53435 <https://github.com/ceph/ceph/pull/53435>`_, Zac Dover)
* doc/architecture: edit "Data Scrubbing" (`pr#53731 <https://github.com/ceph/ceph/pull/53731>`_, Zac Dover)
* doc/architecture: Edit "HA Auth" (`pr#53489 <https://github.com/ceph/ceph/pull/53489>`_, Zac Dover)
* doc/architecture: edit "HA Authentication" (`pr#53633 <https://github.com/ceph/ceph/pull/53633>`_, Zac Dover)
* doc/architecture: edit "High Avail. Monitors" (`pr#53452 <https://github.com/ceph/ceph/pull/53452>`_, Zac Dover)
* doc/architecture: edit "OSD Membership and Status" (`pr#53728 <https://github.com/ceph/ceph/pull/53728>`_, Zac Dover)
* doc/architecture: edit "OSDs service clients directly" (`pr#53687 <https://github.com/ceph/ceph/pull/53687>`_, Zac Dover)
* doc/architecture: edit "Peering and Sets" (`pr#53872 <https://github.com/ceph/ceph/pull/53872>`_, Zac Dover)
* doc/architecture: edit "Replication" (`pr#53739 <https://github.com/ceph/ceph/pull/53739>`_, Zac Dover)
* doc/architecture: edit "SDEH" (`pr#53660 <https://github.com/ceph/ceph/pull/53660>`_, Zac Dover)
* doc/architecture: edit several sections (`pr#53743 <https://github.com/ceph/ceph/pull/53743>`_, Zac Dover)
* doc/architecture: repair RBD sentence (`pr#53878 <https://github.com/ceph/ceph/pull/53878>`_, Zac Dover)
* doc/cephadm: add ssh note to install.rst (`pr#53200 <https://github.com/ceph/ceph/pull/53200>`_, Zac Dover)
* doc/cephadm: edit "Adding Hosts" in install.rst (`pr#53226 <https://github.com/ceph/ceph/pull/53226>`_, Zac Dover)
* doc/cephadm: edit sentence in mgr.rst (`pr#53165 <https://github.com/ceph/ceph/pull/53165>`_, Zac Dover)
* doc/cephadm: fix typo in cephadm initial crush location section (`pr#52888 <https://github.com/ceph/ceph/pull/52888>`_, John Mulligan)
* doc/cephfs: add note to isolate metadata pool osds (`pr#52464 <https://github.com/ceph/ceph/pull/52464>`_, Patrick Donnelly)
* doc/cephfs: edit fs-volumes.rst (1 of x) (`pr#51466 <https://github.com/ceph/ceph/pull/51466>`_, Zac Dover)
* doc/cephfs: explain cephfs data and metadata set (`pr#51236 <https://github.com/ceph/ceph/pull/51236>`_, Zac Dover)
* doc/cephfs: fix prompts in fs-volumes.rst (`pr#51435 <https://github.com/ceph/ceph/pull/51435>`_, Zac Dover)
* doc/cephfs: Improve fs-volumes.rst (`pr#50831 <https://github.com/ceph/ceph/pull/50831>`_, Anthony D'Atri)
* doc/cephfs: line-edit "Mirroring Module" (`pr#51543 <https://github.com/ceph/ceph/pull/51543>`_, Zac Dover)
* doc/cephfs: rectify prompts in fs-volumes.rst (`pr#51459 <https://github.com/ceph/ceph/pull/51459>`_, Zac Dover)
* doc/cephfs: repairing inaccessible FSes (`pr#51372 <https://github.com/ceph/ceph/pull/51372>`_, Zac Dover)
* doc/cephfs: write cephfs commands fully in docs (`pr#53401 <https://github.com/ceph/ceph/pull/53401>`_, Rishabh Dave)
* doc/configuration: edit "bg" in mon-config-ref.rst (`pr#53348 <https://github.com/ceph/ceph/pull/53348>`_, Zac Dover)
* doc/dev/encoding.txt: update per std::optional (`pr#51398 <https://github.com/ceph/ceph/pull/51398>`_, Radoslaw Zarzynski)
* doc/dev: backport deduplication.rst to Quincy (`pr#53533 <https://github.com/ceph/ceph/pull/53533>`_, Zac Dover)
* doc/dev: fix "deploying dev cluster" link (`pr#52035 <https://github.com/ceph/ceph/pull/52035>`_, Zac Dover)
* doc/dev: Fix typos in files cephfs-mirroring.rst and deduplication.rst (`pr#53541 <https://github.com/ceph/ceph/pull/53541>`_, Daniel Parkes)
* doc/dev: format command in cephfs-mirroring (`pr#51108 <https://github.com/ceph/ceph/pull/51108>`_, Zac Dover)
* doc/dev: remove seqdiag assets (`pr#52310 <https://github.com/ceph/ceph/pull/52310>`_, Zac Dover)
* doc/foundation: Updating foundation members for July 2023 (`pr#54064 <https://github.com/ceph/ceph/pull/54064>`_, Mike Perez)
* doc/glossary: add "Hybrid Storage" (`pr#51097 <https://github.com/ceph/ceph/pull/51097>`_, Zac Dover)
* doc/glossary: add "primary affinity" to glossary (`pr#53428 <https://github.com/ceph/ceph/pull/53428>`_, Zac Dover)
* doc/glossary: add "Scrubbing" (`pr#50702 <https://github.com/ceph/ceph/pull/50702>`_, Zac Dover)
* doc/glossary: add "User" (`pr#50672 <https://github.com/ceph/ceph/pull/50672>`_, Zac Dover)
* doc/glossary: improve "CephX" entry (`pr#51064 <https://github.com/ceph/ceph/pull/51064>`_, Zac Dover)
* doc/glossary: link to CephX Config ref (`pr#50708 <https://github.com/ceph/ceph/pull/50708>`_, Zac Dover)
* doc/glossary: update bluestore entry (`pr#51694 <https://github.com/ceph/ceph/pull/51694>`_, Zac Dover)
* doc/man/8: improve radosgw-admin.rst (`pr#53268 <https://github.com/ceph/ceph/pull/53268>`_, Anthony D'Atri)
* doc/man: radosgw-admin.rst typo (`pr#53316 <https://github.com/ceph/ceph/pull/53316>`_, Zac Dover)
* doc/man: remove docs about support for unix domain sockets (`pr#53313 <https://github.com/ceph/ceph/pull/53313>`_, Zac Dover)
* doc/mgr/ceph_api: Promptify example commands in index.rst (`pr#52696 <https://github.com/ceph/ceph/pull/52696>`_, Ville Ojamo)
* doc/mgr/dashboard: fix a typo (`pr#52142 <https://github.com/ceph/ceph/pull/52142>`_, Guido Santella)
* doc/mgr/prometheus: fix confval reference (`pr#51093 <https://github.com/ceph/ceph/pull/51093>`_, Piotr Parczewski)
* doc/mgr/rgw.rst: add missing "ceph" command in cli specification (`pr#52487 <https://github.com/ceph/ceph/pull/52487>`_, Ville Ojamo)
* doc/mgr/rgw.rst: multisite typed wrong (`pr#52479 <https://github.com/ceph/ceph/pull/52479>`_, Ville Ojamo)
* doc/mgr: edit "leaderboard" in telemetry.rst (`pr#51721 <https://github.com/ceph/ceph/pull/51721>`_, Zac Dover)
* doc/mgr: update prompts in prometheus.rst (`pr#51310 <https://github.com/ceph/ceph/pull/51310>`_, Zac Dover)
* doc/msgr2: update dual stack status (`pr#50800 <https://github.com/ceph/ceph/pull/50800>`_, Dan van der Ster)
* doc/operations: fix prompt in bluestore-migration (`pr#50662 <https://github.com/ceph/ceph/pull/50662>`_, Zac Dover)
* doc/rados/config: edit auth-config-ref (`pr#50950 <https://github.com/ceph/ceph/pull/50950>`_, Zac Dover)
* doc/rados/configuration: add links to MON DNS (`pr#52613 <https://github.com/ceph/ceph/pull/52613>`_, Ville Ojamo)
* doc/rados/configuration: Avoid repeating "support" in msgr2.rst (`pr#52999 <https://github.com/ceph/ceph/pull/52999>`_, Ville Ojamo)
* doc/rados/operations: Acting Set question (`pr#51740 <https://github.com/ceph/ceph/pull/51740>`_, Zac Dover)
* doc/rados/operations: edit monitoring.rst (`pr#51036 <https://github.com/ceph/ceph/pull/51036>`_, Zac Dover)
* doc/rados/operations: Fix erasure-code-jerasure.rst fix (`pr#51743 <https://github.com/ceph/ceph/pull/51743>`_, Anthony D'Atri)
* doc/rados/operations: fix typo in balancer.rst (`pr#51938 <https://github.com/ceph/ceph/pull/51938>`_, Pierre Riteau)
* doc/rados/operations: Fix typo in erasure-code.rst (`pr#50752 <https://github.com/ceph/ceph/pull/50752>`_, Sainithin Artham)
* doc/rados/operations: Improve formatting in crush-map.rst (`pr#52140 <https://github.com/ceph/ceph/pull/52140>`_, Anthony D'Atri)
* doc/rados/ops: add ceph-medic documentation (`pr#50853 <https://github.com/ceph/ceph/pull/50853>`_, Zac Dover)
* doc/rados/ops: add hyphen to mon-osd-pg.rst (`pr#50960 <https://github.com/ceph/ceph/pull/50960>`_, Zac Dover)
* doc/rados/ops: edit health checks.rst (5 of x) (`pr#50967 <https://github.com/ceph/ceph/pull/50967>`_, Zac Dover)
* doc/rados/ops: edit health-checks.rst (1 of x) (`pr#50797 <https://github.com/ceph/ceph/pull/50797>`_, Zac Dover)
* doc/rados/ops: edit health-checks.rst (2 of x) (`pr#50912 <https://github.com/ceph/ceph/pull/50912>`_, Zac Dover)
* doc/rados/ops: edit health-checks.rst (3 of x) (`pr#50953 <https://github.com/ceph/ceph/pull/50953>`_, Zac Dover)
* doc/rados/ops: edit health-checks.rst (4 of x) (`pr#50956 <https://github.com/ceph/ceph/pull/50956>`_, Zac Dover)
* doc/rados/ops: edit health-checks.rst (6 of x) (`pr#50970 <https://github.com/ceph/ceph/pull/50970>`_, Zac Dover)
* doc/rados/ops: edit monitoring-osd-pg.rst (1 of x) (`pr#50865 <https://github.com/ceph/ceph/pull/50865>`_, Zac Dover)
* doc/rados/ops: edit monitoring-osd-pg.rst (2 of x) (`pr#50946 <https://github.com/ceph/ceph/pull/50946>`_, Zac Dover)
* doc/rados/ops: edit user-management.rst (3 of x) (`pr#51240 <https://github.com/ceph/ceph/pull/51240>`_, Zac Dover)
* doc/rados/ops: line-edit operating.rst (`pr#50934 <https://github.com/ceph/ceph/pull/50934>`_, Zac Dover)
* doc/rados/ops: remove ceph-medic from monitoring (`pr#51088 <https://github.com/ceph/ceph/pull/51088>`_, Zac Dover)
* doc/rados: add bulk flag to pools.rst (`pr#53318 <https://github.com/ceph/ceph/pull/53318>`_, Zac Dover)
* doc/rados: add link to ops/health-checks.rst (`pr#50762 <https://github.com/ceph/ceph/pull/50762>`_, Zac Dover)
* doc/rados: add math markup to placement-groups.rst (`pr#52038 <https://github.com/ceph/ceph/pull/52038>`_, Zac Dover)
* doc/rados: clean up ops/bluestore-migration.rst (`pr#50678 <https://github.com/ceph/ceph/pull/50678>`_, Zac Dover)
* doc/rados: edit add-or-rm-osds (1 of x) (`pr#52384 <https://github.com/ceph/ceph/pull/52384>`_, Zac Dover)
* doc/rados: edit add-or-rm-osds (2 of x) (`pr#52451 <https://github.com/ceph/ceph/pull/52451>`_, Zac Dover)
* doc/rados: edit balancer.rst (`pr#51825 <https://github.com/ceph/ceph/pull/51825>`_, Zac Dover)
* doc/rados: edit bluestore-config-ref.rst (1 of x) (`pr#51790 <https://github.com/ceph/ceph/pull/51790>`_, Zac Dover)
* doc/rados: edit bluestore-config-ref.rst (2 of x) (`pr#51793 <https://github.com/ceph/ceph/pull/51793>`_, Zac Dover)
* doc/rados: edit ceph-conf.rst (`pr#52449 <https://github.com/ceph/ceph/pull/52449>`_, Zac Dover)
* doc/rados: edit ceph-conf.rst (2 of x) (`pr#52471 <https://github.com/ceph/ceph/pull/52471>`_, Zac Dover)
* doc/rados: edit ceph-conf.rst (3 of x) (`pr#52589 <https://github.com/ceph/ceph/pull/52589>`_, Zac Dover)
* doc/rados: edit ceph-conf.rst (4 of x) (`pr#52594 <https://github.com/ceph/ceph/pull/52594>`_, Zac Dover)
* doc/rados: edit change-mon-elections (`pr#51999 <https://github.com/ceph/ceph/pull/51999>`_, Zac Dover)
* doc/rados: edit control.rst (1 of x) (`pr#52153 <https://github.com/ceph/ceph/pull/52153>`_, Zac Dover)
* doc/rados: edit crush-map-edits (2 of x) (`pr#52312 <https://github.com/ceph/ceph/pull/52312>`_, Zac Dover)
* doc/rados: edit crush-map-edits.rst (1 of x) (`pr#52180 <https://github.com/ceph/ceph/pull/52180>`_, Zac Dover)
* doc/rados: edit crush-map.rst (1 of x) (`pr#52031 <https://github.com/ceph/ceph/pull/52031>`_, Zac Dover)
* doc/rados: edit crush-map.rst (2 of x) (`pr#52070 <https://github.com/ceph/ceph/pull/52070>`_, Zac Dover)
* doc/rados: edit crush-map.rst (3 of x) (`pr#52094 <https://github.com/ceph/ceph/pull/52094>`_, Zac Dover)
* doc/rados: edit crush-map.rst (4 of x) (`pr#52099 <https://github.com/ceph/ceph/pull/52099>`_, Zac Dover)
* doc/rados: edit data-placement.rst (`pr#51596 <https://github.com/ceph/ceph/pull/51596>`_, Zac Dover)
* doc/rados: edit devices.rst (`pr#51478 <https://github.com/ceph/ceph/pull/51478>`_, Zac Dover)
* doc/rados: edit filestore-config-ref.rst (`pr#51752 <https://github.com/ceph/ceph/pull/51752>`_, Zac Dover)
* doc/rados: edit firefly tunables section (`pr#52103 <https://github.com/ceph/ceph/pull/52103>`_, Zac Dover)
* doc/rados: edit log-and-debug.rst (1 of x) (`pr#51903 <https://github.com/ceph/ceph/pull/51903>`_, Zac Dover)
* doc/rados: edit log-and-debug.rst (2 of x) (`pr#51907 <https://github.com/ceph/ceph/pull/51907>`_, Zac Dover)
* doc/rados: edit memory-profiling.rst (`pr#53933 <https://github.com/ceph/ceph/pull/53933>`_, Zac Dover)
* doc/rados: edit operations/add-or-rm-mons (1 of x) (`pr#52890 <https://github.com/ceph/ceph/pull/52890>`_, Zac Dover)
* doc/rados: edit operations/add-or-rm-mons (2 of x) (`pr#52826 <https://github.com/ceph/ceph/pull/52826>`_, Zac Dover)
* doc/rados: edit operations/bs-migration (1 of x) (`pr#50587 <https://github.com/ceph/ceph/pull/50587>`_, Zac Dover)
* doc/rados: edit operations/bs-migration (2 of x) (`pr#50590 <https://github.com/ceph/ceph/pull/50590>`_, Zac Dover)
* doc/rados: edit ops/control.rst (1 of x) (`pr#53812 <https://github.com/ceph/ceph/pull/53812>`_, zdover23, Zac Dover)
* doc/rados: edit ops/control.rst (2 of x) (`pr#53816 <https://github.com/ceph/ceph/pull/53816>`_, Zac Dover)
* doc/rados: edit ops/monitoring.rst (1 of 3) (`pr#50823 <https://github.com/ceph/ceph/pull/50823>`_, Zac Dover)
* doc/rados: edit ops/monitoring.rst (2 of 3) (`pr#50849 <https://github.com/ceph/ceph/pull/50849>`_, Zac Dover)
* doc/rados: edit placement-groups.rst (1 of x) (`pr#51985 <https://github.com/ceph/ceph/pull/51985>`_, Zac Dover)
* doc/rados: edit placement-groups.rst (2 of x) (`pr#51997 <https://github.com/ceph/ceph/pull/51997>`_, Zac Dover)
* doc/rados: edit placement-groups.rst (3 of x) (`pr#52002 <https://github.com/ceph/ceph/pull/52002>`_, Zac Dover)
* doc/rados: edit pools.rst (1 of x) (`pr#51913 <https://github.com/ceph/ceph/pull/51913>`_, Zac Dover)
* doc/rados: edit pools.rst (2 of x) (`pr#51940 <https://github.com/ceph/ceph/pull/51940>`_, Zac Dover)
* doc/rados: edit pools.rst (3 of x) (`pr#51957 <https://github.com/ceph/ceph/pull/51957>`_, Zac Dover)
* doc/rados: edit pools.rst (4 of x) (`pr#51971 <https://github.com/ceph/ceph/pull/51971>`_, Zac Dover)
* doc/rados: edit stretch-mode procedure (`pr#51290 <https://github.com/ceph/ceph/pull/51290>`_, Zac Dover)
* doc/rados: edit stretch-mode.rst (`pr#51338 <https://github.com/ceph/ceph/pull/51338>`_, Zac Dover)
* doc/rados: edit stretch-mode.rst (`pr#51303 <https://github.com/ceph/ceph/pull/51303>`_, Zac Dover)
* doc/rados: edit troubleshooting-mon.rst (1 of x) (`pr#51905 <https://github.com/ceph/ceph/pull/51905>`_, Zac Dover)
* doc/rados: edit troubleshooting-mon.rst (2 of x) (`pr#52840 <https://github.com/ceph/ceph/pull/52840>`_, Zac Dover)
* doc/rados: edit troubleshooting-mon.rst (3 of x) (`pr#53880 <https://github.com/ceph/ceph/pull/53880>`_, Zac Dover)
* doc/rados: edit troubleshooting-mon.rst (4 of x) (`pr#53898 <https://github.com/ceph/ceph/pull/53898>`_, Zac Dover)
* doc/rados: edit troubleshooting-osd (1 of x) (`pr#53983 <https://github.com/ceph/ceph/pull/53983>`_, Zac Dover)
* doc/rados: Edit troubleshooting-osd (2 of x) (`pr#54001 <https://github.com/ceph/ceph/pull/54001>`_, Zac Dover)
* doc/rados: Edit troubleshooting-osd (3 of x) (`pr#54027 <https://github.com/ceph/ceph/pull/54027>`_, Zac Dover)
* doc/rados: edit troubleshooting-pg (2 of x) (`pr#54115 <https://github.com/ceph/ceph/pull/54115>`_, Zac Dover)
* doc/rados: edit troubleshooting-pg.rst (1 of x) (`pr#54074 <https://github.com/ceph/ceph/pull/54074>`_, Zac Dover)
* doc/rados: edit troubleshooting.rst (`pr#53838 <https://github.com/ceph/ceph/pull/53838>`_, Zac Dover)
* doc/rados: edit troubleshooting/community.rst (`pr#53882 <https://github.com/ceph/ceph/pull/53882>`_, Zac Dover)
* doc/rados: edit user-management (2 of x) (`pr#51156 <https://github.com/ceph/ceph/pull/51156>`_, Zac Dover)
* doc/rados: edit user-management.rst (1 of x) (`pr#50641 <https://github.com/ceph/ceph/pull/50641>`_, Zac Dover)
* doc/rados: fix link in common.rst (`pr#51756 <https://github.com/ceph/ceph/pull/51756>`_, Zac Dover)
* doc/rados: fix list in crush-map.rst (`pr#52066 <https://github.com/ceph/ceph/pull/52066>`_, Zac Dover)
* doc/rados: fix typos in pg-repair.rst (`pr#51898 <https://github.com/ceph/ceph/pull/51898>`_, Zac Dover)
* doc/rados: introduce emdash (`pr#52382 <https://github.com/ceph/ceph/pull/52382>`_, Zac Dover)
* doc/rados: line edit mon-lookup-dns top matter (`pr#50582 <https://github.com/ceph/ceph/pull/50582>`_, Zac Dover)
* doc/rados: line-edit common.rst (`pr#50943 <https://github.com/ceph/ceph/pull/50943>`_, Zac Dover)
* doc/rados: line-edit devices.rst (`pr#51577 <https://github.com/ceph/ceph/pull/51577>`_, Zac Dover)
* doc/rados: line-edit erasure-code.rst (`pr#50619 <https://github.com/ceph/ceph/pull/50619>`_, Zac Dover)
* doc/rados: line-edit pg-repair.rst (`pr#50803 <https://github.com/ceph/ceph/pull/50803>`_, Zac Dover)
* doc/rados: line-edit upmap.rst (`pr#50566 <https://github.com/ceph/ceph/pull/50566>`_, Zac Dover)
* doc/rados: m-config-ref: edit "background" (`pr#51273 <https://github.com/ceph/ceph/pull/51273>`_, Zac Dover)
* doc/rados: pools.rst: "decreaesed" (`pr#51920 <https://github.com/ceph/ceph/pull/51920>`_, Zac Dover)
* doc/rados: remove git tag in placement-groups in q (`pr#51990 <https://github.com/ceph/ceph/pull/51990>`_, Zac Dover)
* doc/rados: stretch-mode.rst (other commands) (`pr#51390 <https://github.com/ceph/ceph/pull/51390>`_, Zac Dover)
* doc/rados: stretch-mode: stretch cluster issues (`pr#51378 <https://github.com/ceph/ceph/pull/51378>`_, Zac Dover)
* doc/rados: update monitoring-osd-pg.rst (`pr#52959 <https://github.com/ceph/ceph/pull/52959>`_, Zac Dover)
* doc/radosgw: Add missing space to date option spec in admin.rst (`pr#52694 <https://github.com/ceph/ceph/pull/52694>`_, Ville Ojamo)
* doc/radosgw: add Zonegroup policy explanation (`pr#52362 <https://github.com/ceph/ceph/pull/52362>`_, Zac Dover)
* doc/radosgw: add Zonegroup purpose (`pr#52349 <https://github.com/ceph/ceph/pull/52349>`_, Zac Dover)
* doc/radosgw: correct emphasis in rate limit section (`pr#52713 <https://github.com/ceph/ceph/pull/52713>`_, Piotr Parczewski)
* doc/radosgw: edit "Basic Workflow" in s3select.rst (`pr#52263 <https://github.com/ceph/ceph/pull/52263>`_, Zac Dover)
* doc/radosgw: edit "Overview" in s3select.rst (`pr#52220 <https://github.com/ceph/ceph/pull/52220>`_, Zac Dover)
* doc/radosgw: explain multisite dynamic sharding (`pr#51586 <https://github.com/ceph/ceph/pull/51586>`_, Zac Dover)
* doc/radosgw: fix command error blank (`pr#53656 <https://github.com/ceph/ceph/pull/53656>`_, stevenhua)
* doc/radosgw: format part of s3select (`pr#51117 <https://github.com/ceph/ceph/pull/51117>`_, Cole Mitchell)
* doc/radosgw: format part of s3select (`pr#51105 <https://github.com/ceph/ceph/pull/51105>`_, Cole Mitchell)
* doc/radosgw: Improve language and formatting in config-ref.rst (`pr#52836 <https://github.com/ceph/ceph/pull/52836>`_, Ville Ojamo)
* doc/radosgw: multisite - edit "migrating a single-site" (`pr#53262 <https://github.com/ceph/ceph/pull/53262>`_, Qi Tao)
* doc/radosgw: rabbitmq - push-endpoint edit (`pr#51306 <https://github.com/ceph/ceph/pull/51306>`_, Zac Dover)
* doc/radosgw: refine "Zones" in multisite.rst (`pr#52282 <https://github.com/ceph/ceph/pull/52282>`_, Zac Dover)
* doc/radosgw: remove pipes from s3select.rst (`pr#52188 <https://github.com/ceph/ceph/pull/52188>`_, Zac Dover)
* doc/radosgw: remove pipes from s3select.rst (`pr#52184 <https://github.com/ceph/ceph/pull/52184>`_, Zac Dover)
* doc/radosgw: s/s3select/S3 Select/ (`pr#52279 <https://github.com/ceph/ceph/pull/52279>`_, Zac Dover)
* doc/radosgw: update rate limit management (`pr#52911 <https://github.com/ceph/ceph/pull/52911>`_, Zac Dover)
* doc/README.md - edit "Building Ceph" (`pr#53058 <https://github.com/ceph/ceph/pull/53058>`_, Zac Dover)
* doc/README.md - improve "Running a test cluster" (`pr#53259 <https://github.com/ceph/ceph/pull/53259>`_, Zac Dover)
* doc/rgw/lua: add info uploading a script in cephadm deployment (`pr#52299 <https://github.com/ceph/ceph/pull/52299>`_, Yuval Lifshitz)
* doc/rgw: refine "Setting a Zonegroup" (`pr#51072 <https://github.com/ceph/ceph/pull/51072>`_, Zac Dover)
* doc/rgw: several response headers are supported (`pr#52804 <https://github.com/ceph/ceph/pull/52804>`_, Casey Bodley)
* doc/start/os-recommendations: drop 4.14 kernel and reword guidance (`pr#51490 <https://github.com/ceph/ceph/pull/51490>`_, Ilya Dryomov)
* doc/start: documenting-ceph - add squash procedure (`pr#50740 <https://github.com/ceph/ceph/pull/50740>`_, Zac Dover)
* doc/start: edit first 150 lines of documenting-ceph (`pr#51182 <https://github.com/ceph/ceph/pull/51182>`_, Zac Dover)
* doc/start: edit os-recommendations.rst (`pr#53180 <https://github.com/ceph/ceph/pull/53180>`_, Zac Dover)
* doc/start: fix "Planet Ceph" link (`pr#51420 <https://github.com/ceph/ceph/pull/51420>`_, Zac Dover)
* doc/start: format procedure in documenting-ceph (`pr#50788 <https://github.com/ceph/ceph/pull/50788>`_, Zac Dover)
* doc/start: KRBD feature flag support note (`pr#51503 <https://github.com/ceph/ceph/pull/51503>`_, Zac Dover)
* doc/start: Modernize and clarify hardware-recommendations.rst (`pr#54072 <https://github.com/ceph/ceph/pull/54072>`_, Anthony D'Atri)
* doc/start: rewrite intro paragraph (`pr#51221 <https://github.com/ceph/ceph/pull/51221>`_, Zac Dover)
* doc/start: update "notify us" section (`pr#50770 <https://github.com/ceph/ceph/pull/50770>`_, Zac Dover)
* doc/start: update linking conventions (`pr#52913 <https://github.com/ceph/ceph/pull/52913>`_, Zac Dover)
* doc/start: update linking conventions (`pr#52842 <https://github.com/ceph/ceph/pull/52842>`_, Zac Dover)
* doc/troubleshooting: edit cpu-profiling.rst (`pr#53060 <https://github.com/ceph/ceph/pull/53060>`_, Zac Dover)
* doc: Add a note on possible deadlock on volume deletion (`pr#52947 <https://github.com/ceph/ceph/pull/52947>`_, Kotresh HR)
* doc: add information on expediting MDS recovery (`pr#52368 <https://github.com/ceph/ceph/pull/52368>`_, Patrick Donnelly)
* doc: add link to "documenting ceph" to index.rst (`pr#51470 <https://github.com/ceph/ceph/pull/51470>`_, Zac Dover)
* doc: Add missing `ceph` command in documentation section `REPLACING A… (`pr#51620 <https://github.com/ceph/ceph/pull/51620>`_, Alexander Proschek)
* doc: add note for removing (automatic) partitioning policy (`pr#53570 <https://github.com/ceph/ceph/pull/53570>`_, Venky Shankar)
* doc: Add warning on manual CRUSH rule removal (`pr#53421 <https://github.com/ceph/ceph/pull/53421>`_, Alvin Owyong)
* doc: deprecate the cache tiering (`pr#51653 <https://github.com/ceph/ceph/pull/51653>`_, Radosław Zarzyński)
* doc: Documentation about main Ceph metrics (`pr#54112 <https://github.com/ceph/ceph/pull/54112>`_, Juan Miguel Olmo Martínez)
* doc: edit README.md - contributing code (`pr#53050 <https://github.com/ceph/ceph/pull/53050>`_, Zac Dover)
* doc: expand and consolidate mds placement (`pr#53147 <https://github.com/ceph/ceph/pull/53147>`_, Patrick Donnelly)
* doc: explain cephfs mirroring `peer_add` step in detail (`pr#51521 <https://github.com/ceph/ceph/pull/51521>`_, Venky Shankar)
* doc: Fix doc for mds cap acquisition throttle (`pr#53025 <https://github.com/ceph/ceph/pull/53025>`_, Kotresh HR)
* doc: for EC we recommend K+1 (`pr#52780 <https://github.com/ceph/ceph/pull/52780>`_, Dan van der Ster)
* doc: governance.rst - update D Orman (`pr#52573 <https://github.com/ceph/ceph/pull/52573>`_, Zac Dover)
* doc: improve doc/dev/encoding.rst (`pr#52759 <https://github.com/ceph/ceph/pull/52759>`_, Radosław Zarzyński)
* doc: improve submodule update command - README.md (`pr#53001 <https://github.com/ceph/ceph/pull/53001>`_, Zac Dover)
* doc: remove egg fragment from dev/developer_guide/running-tests-locally (`pr#53854 <https://github.com/ceph/ceph/pull/53854>`_, Dhairya Parmar)
* doc: Update jerasure.org references (`pr#51726 <https://github.com/ceph/ceph/pull/51726>`_, Anthony D'Atri)
* doc: Update mClock QOS documentation to discard osd_mclock_cost_per\_\* (`pr#54080 <https://github.com/ceph/ceph/pull/54080>`_, tanchangzhi)
* doc: update multisite doc (`pr#51401 <https://github.com/ceph/ceph/pull/51401>`_, parth-gr)
* doc: update rados.cc (`pr#52968 <https://github.com/ceph/ceph/pull/52968>`_, Zac Dover)
* doc: update README.md (`pr#52642 <https://github.com/ceph/ceph/pull/52642>`_, Zac Dover)
* doc: update README.md install procedure (`pr#52680 <https://github.com/ceph/ceph/pull/52680>`_, Zac Dover)
* doc: update test cluster commands in README.md (`pr#53350 <https://github.com/ceph/ceph/pull/53350>`_, Zac Dover)
* doc: Use `ceph osd crush tree` command to display weight set weights (`pr#51350 <https://github.com/ceph/ceph/pull/51350>`_, James Lakin)
* docs: fix nfs cluster create syntax (`pr#52424 <https://github.com/ceph/ceph/pull/52424>`_, Paul Cuzner)
* docs: Update the Prometheus endpoint info (`pr#51287 <https://github.com/ceph/ceph/pull/51287>`_, Paul Cuzner)
* Fix FTBFS on gcc 13 (`pr#52120 <https://github.com/ceph/ceph/pull/52120>`_, Tim Serong)
* install-deps: remove the legacy resolver flags (`pr#53706 <https://github.com/ceph/ceph/pull/53706>`_, Nizamudeen A)
* kv/RocksDBStore: Add CompactOnDeletion support (`pr#50893 <https://github.com/ceph/ceph/pull/50893>`_, Mark Nelson)
* kv/RocksDBStore: cumulative backport for rm_range_keys and around (`pr#50636 <https://github.com/ceph/ceph/pull/50636>`_, Igor Fedotov)
* kv/RocksDBStore: don't use real wholespace iterator for prefixed access (`pr#50495 <https://github.com/ceph/ceph/pull/50495>`_, Igor Fedotov)
* libcephsqlite: fill 0s in unread portion of buffer (`pr#53102 <https://github.com/ceph/ceph/pull/53102>`_, Patrick Donnelly)
* librados: aio operate functions can set times (`pr#52118 <https://github.com/ceph/ceph/pull/52118>`_, Casey Bodley)
* librbd/managed_lock/GetLockerRequest: Fix no valid lockers case (`pr#52288 <https://github.com/ceph/ceph/pull/52288>`_, Ilya Dryomov, Matan Breizman)
* librbd: avoid decrementing iterator before first element (`pr#51854 <https://github.com/ceph/ceph/pull/51854>`_, Lucian Petrut)
* librbd: avoid object map corruption in snapshots taken under I/O (`pr#52286 <https://github.com/ceph/ceph/pull/52286>`_, Ilya Dryomov)
* librbd: don't wait for a watch in send_acquire_lock() if client is blocklisted (`pr#50920 <https://github.com/ceph/ceph/pull/50920>`_, Ilya Dryomov, Christopher Hoffman)
* librbd: fix wrong attribute for rbd_quiesce_complete api (`pr#50873 <https://github.com/ceph/ceph/pull/50873>`_, Dongsheng Yang)
* librbd: kick ExclusiveLock state machine on client being blocklisted when waiting for lock (`pr#53294 <https://github.com/ceph/ceph/pull/53294>`_, Ramana Raja)
* librbd: kick ExclusiveLock state machine stalled waiting for lock from reacquire_lock() (`pr#53920 <https://github.com/ceph/ceph/pull/53920>`_, Ramana Raja)
* librbd: localize snap_remove op for mirror snapshots (`pr#51428 <https://github.com/ceph/ceph/pull/51428>`_, Christopher Hoffman)
* librbd: make CreatePrimaryRequest remove any unlinked mirror snapshots (`pr#53275 <https://github.com/ceph/ceph/pull/53275>`_, Ilya Dryomov)
* librbd: remove previous incomplete primary snapshot after successfully creating a new one (`pr#51173 <https://github.com/ceph/ceph/pull/51173>`_, Ilya Dryomov, Prasanna Kumar Kalever)
* librbd: report better errors when failing to enable mirroring on an image (`pr#50837 <https://github.com/ceph/ceph/pull/50837>`_, Prasanna Kumar Kalever)
* log: writes to stderr (pipe) may not be atomic (`pr#50777 <https://github.com/ceph/ceph/pull/50777>`_, Lucian Petrut, Patrick Donnelly)
* MDS imported_inodes metric is not updated (`pr#51697 <https://github.com/ceph/ceph/pull/51697>`_, Yongseok Oh)
* mds/FSMap: allow upgrades if no up mds (`pr#53852 <https://github.com/ceph/ceph/pull/53852>`_, Patrick Donnelly)
* mds/Server: mark a cap acquisition throttle event in the request (`pr#53167 <https://github.com/ceph/ceph/pull/53167>`_, Leonid Usov)
* mds: acquire inode snaplock in open (`pr#53184 <https://github.com/ceph/ceph/pull/53184>`_, Patrick Donnelly)
* mds: add event for batching getattr/lookup (`pr#53557 <https://github.com/ceph/ceph/pull/53557>`_, Patrick Donnelly)
* mds: allow unlink from lost+found directory (`issue#59569 <http://tracker.ceph.com/issues/59569>`_, `pr#51689 <https://github.com/ceph/ceph/pull/51689>`_, Venky Shankar)
* mds: blocklist clients with "bloated" session metadata (`issue#61947 <http://tracker.ceph.com/issues/61947>`_, `issue#62873 <http://tracker.ceph.com/issues/62873>`_, `pr#53330 <https://github.com/ceph/ceph/pull/53330>`_, Venky Shankar)
* mds: catch damage to CDentry's first member before persisting (`issue#58482 <http://tracker.ceph.com/issues/58482>`_, `pr#50779 <https://github.com/ceph/ceph/pull/50779>`_, Patrick Donnelly)
* mds: display sane hex value (0x0) for empty feature bit (`pr#52127 <https://github.com/ceph/ceph/pull/52127>`_, Jos Collin)
* mds: do not send split_realms for CEPH_SNAP_OP_UPDATE msg (`pr#52849 <https://github.com/ceph/ceph/pull/52849>`_, Xiubo Li)
* mds: do not take the ino which has been used (`pr#51507 <https://github.com/ceph/ceph/pull/51507>`_, Xiubo Li)
* mds: drop locks and retry when lock set changes (`pr#53242 <https://github.com/ceph/ceph/pull/53242>`_, Patrick Donnelly)
* mds: fix stray evaluation using scrub and introduce new option (`pr#50815 <https://github.com/ceph/ceph/pull/50815>`_, Dhairya Parmar)
* mds: Fix the linkmerge assert check (`pr#52725 <https://github.com/ceph/ceph/pull/52725>`_, Kotresh HR)
* mds: force replay sessionmap version (`pr#50724 <https://github.com/ceph/ceph/pull/50724>`_, Xiubo Li)
* mds: make num_fwd and num_retry to __u32 (`pr#50732 <https://github.com/ceph/ceph/pull/50732>`_, Xiubo Li)
* mds: MDLog::_recovery_thread: handle the errors gracefully (`pr#52514 <https://github.com/ceph/ceph/pull/52514>`_, Jos Collin)
* mds: rdlock_path_xlock_dentry supports returning auth target inode (`pr#51688 <https://github.com/ceph/ceph/pull/51688>`_, Zhansong Gao)
* mds: record and dump last tid for trimming completed requests (or flushes) (`issue#57985 <http://tracker.ceph.com/issues/57985>`_, `pr#50785 <https://github.com/ceph/ceph/pull/50785>`_, Venky Shankar)
* mds: session ls command appears twice in command listing (`pr#52516 <https://github.com/ceph/ceph/pull/52516>`_, Neeraj Pratap Singh)
* mds: skip forwarding request if the session were removed (`pr#52845 <https://github.com/ceph/ceph/pull/52845>`_, Xiubo Li)
* mds: update mdlog perf counters during replay (`pr#52683 <https://github.com/ceph/ceph/pull/52683>`_, Patrick Donnelly)
* mds: wait for unlink operation to finish (`pr#50985 <https://github.com/ceph/ceph/pull/50985>`_, Xiubo Li)
* mds: wait reintegrate to finish when unlinking (`pr#51685 <https://github.com/ceph/ceph/pull/51685>`_, Xiubo Li)
* mgr/cephadm: add commands to set services to managed/unmanaged (`pr#50897 <https://github.com/ceph/ceph/pull/50897>`_, Adam King)
* mgr/cephadm: add more aggressive force flag for host maintenance enter (`pr#50901 <https://github.com/ceph/ceph/pull/50901>`_, Adam King)
* mgr/cephadm: allow configuring anonymous access for grafana (`pr#51617 <https://github.com/ceph/ceph/pull/51617>`_, Adam King)
* mgr/cephadm: allow setting mon crush locations through mon service spec (`pr#51217 <https://github.com/ceph/ceph/pull/51217>`_, Adam King)
* mgr/cephadm: also don't write client files/tuned profiles to maintenance hosts (`pr#53705 <https://github.com/ceph/ceph/pull/53705>`_, Adam King)
* mgr/cephadm: asyncio based universal timeout for ssh/cephadm commands (`pr#51218 <https://github.com/ceph/ceph/pull/51218>`_, Adam King)
* mgr/cephadm: be aware of host's shortname and FQDN (`pr#50888 <https://github.com/ceph/ceph/pull/50888>`_, Adam King)
* mgr/cephadm: don't add mgr into iscsi trusted_ip_list if it's already there (`pr#50521 <https://github.com/ceph/ceph/pull/50521>`_, Mykola Golub)
* mgr/cephadm: handle HostConnectionError when checking for valid addr (`pr#50900 <https://github.com/ceph/ceph/pull/50900>`_, Adam King)
* mgr/cephadm: increasing container stop timeout for OSDs (`pr#50903 <https://github.com/ceph/ceph/pull/50903>`_, Redouane Kachach)
* mgr/cephadm: make upgrade respect use_repo_digest (`pr#50898 <https://github.com/ceph/ceph/pull/50898>`_, Adam King)
* mgr/cephadm: support for nfs backed by VIP (`pr#51616 <https://github.com/ceph/ceph/pull/51616>`_, Adam King)
* mgr/cephadm: update monitoring stack versions (`pr#51356 <https://github.com/ceph/ceph/pull/51356>`_, Nizamudeen A)
* mgr/cephadm: use a dedicated cephadm tmp dir to copy remote files (`pr#50906 <https://github.com/ceph/ceph/pull/50906>`_, Redouane Kachach)
* mgr/dashboard CRUD component backport (`pr#51367 <https://github.com/ceph/ceph/pull/51367>`_, Pedro Gonzalez Gomez, Pere Diaz Bou, Nizamudeen A, Ernesto Puerta)
* mgr/dashboard: Add more decimals in latency graph (`pr#52728 <https://github.com/ceph/ceph/pull/52728>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: add popover to cluster status card (`pr#52027 <https://github.com/ceph/ceph/pull/52027>`_, Nizamudeen A)
* mgr/dashboard: align charts of landing page (`pr#53544 <https://github.com/ceph/ceph/pull/53544>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: allow PUT in CORS (`pr#52706 <https://github.com/ceph/ceph/pull/52706>`_, Nizamudeen A)
* mgr/dashboard: batch backport hackathon prs (`pr#51768 <https://github.com/ceph/ceph/pull/51768>`_, Nizamudeen A, Pedro Gonzalez Gomez, Ankush Behl, Pere Diaz Bou, Aashish Sharma, avanthakkar)
* mgr/dashboard: bump moment from 2.29.3 to 2.29.4 in /src/pybind/mgr/dashboard/frontend (`pr#51358 <https://github.com/ceph/ceph/pull/51358>`_, dependabot[bot])
* mgr/dashboard: disable promote on mirroring not enabled (`pr#52537 <https://github.com/ceph/ceph/pull/52537>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: disable protect if layering is not enabled on the image (`pr#53174 <https://github.com/ceph/ceph/pull/53174>`_, avanthakkar)
* mgr/dashboard: enable protect option if layering enabled (`pr#53796 <https://github.com/ceph/ceph/pull/53796>`_, avanthakkar)
* mgr/dashboard: expose more grafana configs in service form (`pr#51112 <https://github.com/ceph/ceph/pull/51112>`_, Nizamudeen A)
* mgr/dashboard: fix a bug where data would plot wrongly (`pr#52332 <https://github.com/ceph/ceph/pull/52332>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fix cephadm e2e expression changed error (`pr#51079 <https://github.com/ceph/ceph/pull/51079>`_, Nizamudeen A)
* mgr/dashboard: fix CephPGImbalance alert (`pr#51252 <https://github.com/ceph/ceph/pull/51252>`_, Aashish Sharma)
* mgr/dashboard: fix create osd default selected as recommended not working (`pr#51007 <https://github.com/ceph/ceph/pull/51007>`_, Nizamudeen A)
* mgr/dashboard: fix displaying mirror image progress (`pr#50871 <https://github.com/ceph/ceph/pull/50871>`_, Pere Diaz Bou)
* mgr/dashboard: fix eviction of all FS clients (`pr#51011 <https://github.com/ceph/ceph/pull/51011>`_, Pere Diaz Bou)
* mgr/dashboard: fix image columns naming (`pr#53253 <https://github.com/ceph/ceph/pull/53253>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fix issues with read-only user on landing page (`pr#51809 <https://github.com/ceph/ceph/pull/51809>`_, Pedro Gonzalez Gomez, Nizamudeen A)
* mgr/dashboard: Fix rbd snapshot creation (`pr#51076 <https://github.com/ceph/ceph/pull/51076>`_, Aashish Sharma)
* mgr/dashboard: fix regression caused by cephPgImabalance alert (`pr#51525 <https://github.com/ceph/ceph/pull/51525>`_, Aashish Sharma)
* mgr/dashboard: fix rgw page issues when hostname not resolvable (`pr#53216 <https://github.com/ceph/ceph/pull/53216>`_, Nizamudeen A)
* mgr/dashboard: fix test_dashboard_e2e.sh failure (`pr#51866 <https://github.com/ceph/ceph/pull/51866>`_, Nizamudeen A)
* mgr/dashboard: fix the rbd mirroring configure check (`pr#51325 <https://github.com/ceph/ceph/pull/51325>`_, Nizamudeen A)
* mgr/dashboard: fix the rgw roles page (`pr#51867 <https://github.com/ceph/ceph/pull/51867>`_, Nizamudeen A)
* mgr/dashboard: force TLS 1.3 (`pr#50526 <https://github.com/ceph/ceph/pull/50526>`_, Ernesto Puerta)
* mgr/dashboard: hide notification on force promote (`pr#51164 <https://github.com/ceph/ceph/pull/51164>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: images -> edit -> disable checkboxes for layering and deef-flatten (`pr#53387 <https://github.com/ceph/ceph/pull/53387>`_, avanthakkar)
* mgr/dashboard: Landing page v3 (`pr#50608 <https://github.com/ceph/ceph/pull/50608>`_, Pedro Gonzalez Gomez, Nizamudeen A, bryanmontalvan)
* mgr/dashboard: move cephadm e2e cleanup to jenkins job config (`pr#52388 <https://github.com/ceph/ceph/pull/52388>`_, Nizamudeen A)
* mgr/dashboard: n/a entries behind primary snapshot mode (`pr#53225 <https://github.com/ceph/ceph/pull/53225>`_, Pere Diaz Bou)
* mgr/dashboard: paginate hosts (`pr#52917 <https://github.com/ceph/ceph/pull/52917>`_, Pere Diaz Bou)
* mgr/dashboard: rbd-mirror force promotion (`pr#51057 <https://github.com/ceph/ceph/pull/51057>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: remove unncessary hyperlink in landing page (`pr#51119 <https://github.com/ceph/ceph/pull/51119>`_, Nizamudeen A)
* mgr/dashboard: remove used and total used columns in favor of usage bar (`pr#53303 <https://github.com/ceph/ceph/pull/53303>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: set CORS header for unauthorized access (`pr#53203 <https://github.com/ceph/ceph/pull/53203>`_, Nizamudeen A)
* mgr/dashboard: skip Create OSDs step in Cluster expansion (`pr#51149 <https://github.com/ceph/ceph/pull/51149>`_, Nizamudeen A)
* mgr/dashboard: SSO error: AttributeError: 'str' object has no attribute 'decode' (`pr#51952 <https://github.com/ceph/ceph/pull/51952>`_, Volker Theile)
* mgr/nfs: disallow non-existent paths when creating export (`pr#50807 <https://github.com/ceph/ceph/pull/50807>`_, Dhairya Parmar)
* mgr/orchestrator: allow deploying raw mode OSDs with --all-available-devices (`pr#50891 <https://github.com/ceph/ceph/pull/50891>`_, Adam King)
* mgr/orchestrator: fix device size in `orch device ls` output (`pr#50899 <https://github.com/ceph/ceph/pull/50899>`_, Adam King)
* mgr/prometheus: avoid duplicates and deleted entries for rbd_stats_pools (`pr#48523 <https://github.com/ceph/ceph/pull/48523>`_, Avan Thakkar)
* mgr/prometheus: fix pool_objects_repaired and daemon_health_metrics format (`pr#51671 <https://github.com/ceph/ceph/pull/51671>`_, banuchka)
* mgr/rbd_support: add user-friendly stderr message when module is not ready (`pr#52189 <https://github.com/ceph/ceph/pull/52189>`_, Ramana Raja)
* mgr/rbd_support: recover from "double blocklisting" (`pr#51758 <https://github.com/ceph/ceph/pull/51758>`_, Ramana Raja)
* mgr/rbd_support: recover from rados client blocklisting (`pr#51455 <https://github.com/ceph/ceph/pull/51455>`_, Ramana Raja)
* mgr/rgw: initial multisite deployment work (`pr#50887 <https://github.com/ceph/ceph/pull/50887>`_, Redouane Kachach)
* mgr/snap_schedule: add debug log for paths failing snapshot creation (`pr#50780 <https://github.com/ceph/ceph/pull/50780>`_, Milind Changire)
* mgr/snap_schedule: allow retention spec 'n' to be user defined (`pr#52749 <https://github.com/ceph/ceph/pull/52749>`_, Milind Changire, Jakob Haufe)
* mgr/snap_schedule: catch all exceptions for cli (`pr#52752 <https://github.com/ceph/ceph/pull/52752>`_, Milind Changire)
* mgr/telemetry: compile all channels and collections in selftest (`pr#51761 <https://github.com/ceph/ceph/pull/51761>`_, Laura Flores)
* mgr/telemetry: fixed log exceptions as "exception" instead of "error" (`pr#51244 <https://github.com/ceph/ceph/pull/51244>`_, Vonesha Frost)
* mgr/telemetry: make sure histograms are formatted in `all` commands (`pr#50480 <https://github.com/ceph/ceph/pull/50480>`_, Laura Flores)
* mgr/volumes: avoid returning -ESHUTDOWN back to cli (`issue#58651 <http://tracker.ceph.com/issues/58651>`_, `pr#50786 <https://github.com/ceph/ceph/pull/50786>`_, Venky Shankar)
* mgr/volumes: Fix pending_subvolume_deletions in volume info (`pr#53573 <https://github.com/ceph/ceph/pull/53573>`_, Kotresh HR)
* mgr: Add one finisher thread per module (`pr#51044 <https://github.com/ceph/ceph/pull/51044>`_, Kotresh HR, Patrick Donnelly)
* mgr: add urllib3==1.26.15 to mgr/requirements.txt (`pr#51335 <https://github.com/ceph/ceph/pull/51335>`_, Laura Flores)
* mgr: register OSDs in ms_handle_accept (`pr#53188 <https://github.com/ceph/ceph/pull/53188>`_, Patrick Donnelly)
* mgr: store names of modules that register RADOS clients in the MgrMap (`pr#50964 <https://github.com/ceph/ceph/pull/50964>`_, Ramana Raja)
* MgrMonitor: batch commit OSDMap and MgrMap mutations (`pr#50979 <https://github.com/ceph/ceph/pull/50979>`_, Patrick Donnelly, Kefu Chai, Radosław Zarzyński)
* mon, qa: issue pool application warning even if pool is empty (`pr#53042 <https://github.com/ceph/ceph/pull/53042>`_, Prashant D)
* mon/ConfigMonitor: update crush_location from osd entity (`pr#52467 <https://github.com/ceph/ceph/pull/52467>`_, Didier Gazen)
* mon/MDSMonitor: batch last_metadata update with pending (`pr#52228 <https://github.com/ceph/ceph/pull/52228>`_, Patrick Donnelly)
* mon/MDSMonitor: check fscid in pending exists in current (`pr#52234 <https://github.com/ceph/ceph/pull/52234>`_, Patrick Donnelly)
* mon/MDSMonitor: do not propose on error in prepare_update (`pr#52239 <https://github.com/ceph/ceph/pull/52239>`_, Patrick Donnelly)
* mon/MDSMonitor: ignore extraneous up:boot messages (`pr#52243 <https://github.com/ceph/ceph/pull/52243>`_, Patrick Donnelly)
* mon/MDSMonitor: plug paxos when maybe manipulating osdmap (`pr#52983 <https://github.com/ceph/ceph/pull/52983>`_, Patrick Donnelly)
* mon/MonClient: before complete auth with error, reopen session (`pr#52134 <https://github.com/ceph/ceph/pull/52134>`_, Nitzan Mordechai)
* mon/MonClient: resurrect original client_mount_timeout handling (`pr#52534 <https://github.com/ceph/ceph/pull/52534>`_, Ilya Dryomov)
* mon/Monitor.cc: exit function if !osdmon()->is_writeable() && mon/OSDMonitor: Added extra check before mon.go_recovery_stretch_mode() (`pr#51413 <https://github.com/ceph/ceph/pull/51413>`_, Kamoltat)
* mon: avoid exception when setting require-osd-release more than 2 (`pr#51102 <https://github.com/ceph/ceph/pull/51102>`_, Igor Fedotov)
* mon: block osd pool mksnap for fs pools (`pr#52398 <https://github.com/ceph/ceph/pull/52398>`_, Milind Changire)
* mon: Fix ceph versions command (`pr#52161 <https://github.com/ceph/ceph/pull/52161>`_, Prashant D)
* mon: fix iterator mishandling in PGMap::apply_incremental (`pr#52553 <https://github.com/ceph/ceph/pull/52553>`_, Oliver Schmidt)
* msg/async: don't abort when public addrs mismatch bind addrs (`pr#50575 <https://github.com/ceph/ceph/pull/50575>`_, Radosław Zarzyński)
* orchestrator: add `--no-destroy` arg to `ceph orch osd rm` (`pr#51215 <https://github.com/ceph/ceph/pull/51215>`_, Guillaume Abrioux)
* orchestrator: improvements to the orch host ls command (`pr#50889 <https://github.com/ceph/ceph/pull/50889>`_, Paul Cuzner)
* os/bluestore/bluefs: fix dir_link might add link that already exists in compact log (`pr#51002 <https://github.com/ceph/ceph/pull/51002>`_, ethanwu, Adam Kupczyk)
* os/bluestore: Add bluefs write op count metrics (`pr#51777 <https://github.com/ceph/ceph/pull/51777>`_, Joshua Baergen)
* os/bluestore: allow 'fit_to_fast' selector for single-volume osd (`pr#51412 <https://github.com/ceph/ceph/pull/51412>`_, Igor Fedotov)
* os/bluestore: do not signal deleted dirty file to bluefs log (`pr#48171 <https://github.com/ceph/ceph/pull/48171>`_, Igor Fedotov)
* os/bluestore: don't require bluestore_db_block_size when attaching new (`pr#52941 <https://github.com/ceph/ceph/pull/52941>`_, Igor Fedotov)
* os/bluestore: fix no metadata update on truncate+fsync (`pr#48169 <https://github.com/ceph/ceph/pull/48169>`_, Igor Fedotov)
* os/bluestore: fix spillover alert (`pr#50931 <https://github.com/ceph/ceph/pull/50931>`_, Igor Fedotov)
* os/bluestore: log before assert in AvlAllocator (`pr#50319 <https://github.com/ceph/ceph/pull/50319>`_, Igor Fedotov)
* os/bluestore: proper locking for Allocators' dump methods (`pr#48170 <https://github.com/ceph/ceph/pull/48170>`_, Igor Fedotov)
* os/bluestore: proper override rocksdb::WritableFile::Allocate (`pr#51774 <https://github.com/ceph/ceph/pull/51774>`_, Igor Fedotov)
* os/bluestore: report min_alloc_size through "ceph osd metadata" (`pr#50505 <https://github.com/ceph/ceph/pull/50505>`_, Igor Fedotov)
* os/bluestore: use direct write in BlueStore::_write_bdev_label (`pr#48279 <https://github.com/ceph/ceph/pull/48279>`_, luo rixin)
* osd, mon: add pglog dups length (`pr#47840 <https://github.com/ceph/ceph/pull/47840>`_, Nitzan Mordechai)
* osd/OpRequest: Add detailed description for delayed op in osd log file (`pr#53690 <https://github.com/ceph/ceph/pull/53690>`_, Yite Gu)
* osd/OSDCap: allow rbd.metadata_list method under rbd-read-only profile (`pr#51877 <https://github.com/ceph/ceph/pull/51877>`_, Ilya Dryomov)
* osd/PeeringState: fix missed `recheck_readable` from laggy (`pr#49304 <https://github.com/ceph/ceph/pull/49304>`_, 胡玮文)
* osd/scheduler/mClockScheduler: Use same profile and client ids for all clients to ensure allocated QoS limit consumption (`pr#53092 <https://github.com/ceph/ceph/pull/53092>`_, Sridhar Seshasayee)
* osd/scheduler: Reset ephemeral changes to mClock built-in profile (`pr#51664 <https://github.com/ceph/ceph/pull/51664>`_, Sridhar Seshasayee)
* osd/scrub: verify SnapMapper consistency (`pr#52256 <https://github.com/ceph/ceph/pull/52256>`_, Ronen Friedman, Tim Serong, Kefu Chai, Adam C. Emerson)
* osd: bring the missed fmt::formatter for snapid_t to address FTBFS (`pr#54175 <https://github.com/ceph/ceph/pull/54175>`_, Radosław Zarzyński)
* osd: Change scrub cost in case of mClock scheduler (`pr#51728 <https://github.com/ceph/ceph/pull/51728>`_, Aishwarya Mathuria)
* OSD: during test start, not all osds started due to consum map hang (`pr#51807 <https://github.com/ceph/ceph/pull/51807>`_, Nitzan Mordechai)
* OSD: Fix check_past_interval_bounds() (`pr#51512 <https://github.com/ceph/ceph/pull/51512>`_, Matan Breizman, Samuel Just)
* osd: fix: slow scheduling when item_cost is large (`pr#53860 <https://github.com/ceph/ceph/pull/53860>`_, Jrchyang Yu)
* osd: mClock recovery/backfill cost fixes (`pr#49973 <https://github.com/ceph/ceph/pull/49973>`_, Sridhar Seshasayee, Samuel Just)
* osd: set per_pool_stats true when OSD has no PG (`pr#48249 <https://github.com/ceph/ceph/pull/48249>`_, jindengke, lmgdlmgd)
* PendingReleaseNotes: Document mClock scheduler fixes and enhancements (`pr#51978 <https://github.com/ceph/ceph/pull/51978>`_, Sridhar Seshasayee)
* pybind/argparse: blocklist ip validation (`pr#51811 <https://github.com/ceph/ceph/pull/51811>`_, Nitzan Mordechai)
* pybind/mgr/devicehealth: do not crash if db not ready (`pr#52215 <https://github.com/ceph/ceph/pull/52215>`_, Patrick Donnelly)
* pybind/mgr/pg_autoscaler: fix warn when not too few pgs (`pr#53675 <https://github.com/ceph/ceph/pull/53675>`_, Kamoltat)
* pybind/mgr/pg_autoscaler: noautoscale flag retains individual pool configs (`pr#53677 <https://github.com/ceph/ceph/pull/53677>`_, Kamoltat)
* pybind/mgr/pg_autoscaler: Reorderd if statement for the func: _maybe_adjust (`pr#50693 <https://github.com/ceph/ceph/pull/50693>`_, Kamoltat)
* pybind/mgr/pg_autoscaler: Use bytes_used for actual_raw_used (`pr#53725 <https://github.com/ceph/ceph/pull/53725>`_, Kamoltat)
* pybind: drop GIL during library callouts (`pr#52322 <https://github.com/ceph/ceph/pull/52322>`_, Ilya Dryomov, Patrick Donnelly)
* python-common: drive_selection: fix KeyError when osdspec_affinity is not set (`pr#53158 <https://github.com/ceph/ceph/pull/53158>`_, Guillaume Abrioux)
* qa/cephfs: add 'rhel' to family of RH OS in xfstest_dev.py (`pr#52585 <https://github.com/ceph/ceph/pull/52585>`_, Rishabh Dave)
* qa/rgw: add new POOL_APP_NOT_ENABLED failures to log-ignorelist (`pr#53895 <https://github.com/ceph/ceph/pull/53895>`_, Casey Bodley)
* qa/smoke,rados,perf-basic: add POOL_APP_NOT_ENABLED to ignorelist (`pr#54065 <https://github.com/ceph/ceph/pull/54065>`_, Prashant D)
* qa/standalone/osd/divergent-prior.sh: Divergent test 3 with pg_autoscale_mode on pick divergent osd (`pr#52722 <https://github.com/ceph/ceph/pull/52722>`_, Nitzan Mordechai)
* qa/suites/krbd: stress test for recovering from watch errors (`pr#53785 <https://github.com/ceph/ceph/pull/53785>`_, Ilya Dryomov)
* qa/suites/rados: remove rook coverage from the rados suite (`pr#52016 <https://github.com/ceph/ceph/pull/52016>`_, Laura Flores)
* qa/suites/rados: whitelist POOL_APP_NOT_ENABLED for cls tests (`pr#52137 <https://github.com/ceph/ceph/pull/52137>`_, Laura Flores)
* qa/suites/rbd: install qemu-utils in addition to qemu-block-extra on Ubuntu (`pr#51060 <https://github.com/ceph/ceph/pull/51060>`_, Ilya Dryomov)
* qa/suites/upgrade/octopus-x: skip TestClsRbd.mirror_snapshot test (`pr#52992 <https://github.com/ceph/ceph/pull/52992>`_, Ilya Dryomov)
* qa/suites/upgrade/quincy-p2p: skip TestClsRbd.mirror_snapshot test (`pr#53338 <https://github.com/ceph/ceph/pull/53338>`_, Ilya Dryomov)
* qa/suites/{rbd,krbd}: disable POOL_APP_NOT_ENABLED health check (`pr#53598 <https://github.com/ceph/ceph/pull/53598>`_, Ilya Dryomov)
* qa/tasks: Changing default mClock profile to high_recovery_ops (`pr#51568 <https://github.com/ceph/ceph/pull/51568>`_, Aishwarya Mathuria)
* qa/upgrade/quincy-p2p: remove s3tests (`pr#54078 <https://github.com/ceph/ceph/pull/54078>`_, Casey Bodley)
* qa/upgrade: consistently use the tip of the branch as the start version (`pr#50747 <https://github.com/ceph/ceph/pull/50747>`_, Yuri Weinstein)
* qa/workunits/rados/test_dedup_tool.sh: reset dedup tier during tests (`pr#51780 <https://github.com/ceph/ceph/pull/51780>`_, Myoungwon Oh)
* qa: add `POOL_APP_NOT_ENABLED` to ignorelist for cephfs tests (`issue#62508 <http://tracker.ceph.com/issues/62508>`_, `issue#62482 <http://tracker.ceph.com/issues/62482>`_, `pr#53863 <https://github.com/ceph/ceph/pull/53863>`_, Venky Shankar, Patrick Donnelly)
* qa: check each fs for health (`pr#52241 <https://github.com/ceph/ceph/pull/52241>`_, Patrick Donnelly)
* qa: cleanup volumes on unwind (`pr#50766 <https://github.com/ceph/ceph/pull/50766>`_, Patrick Donnelly)
* qa: enable kclient test for newop test (`pr#50991 <https://github.com/ceph/ceph/pull/50991>`_, Xiubo Li, Dhairya Parmar)
* qa: fix cephfs-mirror unwinding and 'fs volume create/rm' order (`pr#52653 <https://github.com/ceph/ceph/pull/52653>`_, Jos Collin)
* qa: ignore expected cluster warning from damage tests (`pr#53485 <https://github.com/ceph/ceph/pull/53485>`_, Patrick Donnelly)
* qa: ignore expected scrub error (`pr#50774 <https://github.com/ceph/ceph/pull/50774>`_, Patrick Donnelly)
* qa: ignore MDS_TRIM warnings when osd thrashing (`pr#50768 <https://github.com/ceph/ceph/pull/50768>`_, Patrick Donnelly)
* qa: output higher debugging for cephfs-journal-tool/cephfs-data-scan (`pr#50772 <https://github.com/ceph/ceph/pull/50772>`_, Patrick Donnelly)
* qa: run scrub post file system recovery (`issue#59527 <http://tracker.ceph.com/issues/59527>`_, `pr#51690 <https://github.com/ceph/ceph/pull/51690>`_, Venky Shankar)
* qa: test_rebuild_simple checks status on wrong file system (`pr#50922 <https://github.com/ceph/ceph/pull/50922>`_, Patrick Donnelly)
* qa: test_recovery_pool uses wrong recovery procedure (`pr#50767 <https://github.com/ceph/ceph/pull/50767>`_, Patrick Donnelly)
* qa: use parallel gzip for compressing logs (`pr#52952 <https://github.com/ceph/ceph/pull/52952>`_, Patrick Donnelly)
* qa: wait for file to have correct size (`pr#52743 <https://github.com/ceph/ceph/pull/52743>`_, Patrick Donnelly)
* qa: wait for MDSMonitor tick to replace daemons (`pr#52236 <https://github.com/ceph/ceph/pull/52236>`_, Patrick Donnelly)
* radosgw-admin: try reshard even if bucket is resharding (`pr#51835 <https://github.com/ceph/ceph/pull/51835>`_, Casey Bodley)
* rbd-mirror: fix image replayer shut down description on force promote (`pr#52879 <https://github.com/ceph/ceph/pull/52879>`_, Prasanna Kumar Kalever)
* rbd-mirror: fix race preventing local image deletion (`pr#52626 <https://github.com/ceph/ceph/pull/52626>`_, N Balachandran)
* rbd-wnbd: improve image map error message (`pr#52289 <https://github.com/ceph/ceph/pull/52289>`_, Lucian Petrut)
* RGW - Fix NoSuchTagSet error (`pr#50103 <https://github.com/ceph/ceph/pull/50103>`_, Daniel Gryniewicz)
* RGW - Use correct multipart upload time (`pr#51834 <https://github.com/ceph/ceph/pull/51834>`_, Daniel Gryniewicz)
* rgw multisite: complete fix for metadata sync issue (`pr#51496 <https://github.com/ceph/ceph/pull/51496>`_, Shilpa Jagannath, gengjichao)
* rgw/admin: 'bucket stats' displays non-empty time (`pr#50485 <https://github.com/ceph/ceph/pull/50485>`_, Casey Bodley)
* rgw/lua: allow bucket name override in pre request (`pr#51300 <https://github.com/ceph/ceph/pull/51300>`_, Yuval Lifshitz)
* rgw/notifications: send mtime in complete multipart upload event (`pr#50962 <https://github.com/ceph/ceph/pull/50962>`_, yuval Lifshitz)
* rgw/notifications: sending metadata in COPY and CompleteMultipartUpload (`pr#49808 <https://github.com/ceph/ceph/pull/49808>`_, yuval Lifshitz)
* rgw/rados: check_quota() uses real bucket owner (`pr#51329 <https://github.com/ceph/ceph/pull/51329>`_, Mykola Golub, Casey Bodley)
* rgw/swift: check position of first slash in slo manifest files (`pr#51598 <https://github.com/ceph/ceph/pull/51598>`_, Marcio Roberto Starke)
* rgw/sync-policy: Correct "sync status" & "sync group" commands (`pr#53396 <https://github.com/ceph/ceph/pull/53396>`_, Soumya Koduri)
* rgw: add radosgw-admin bucket check olh/unlinked commands (`pr#53821 <https://github.com/ceph/ceph/pull/53821>`_, Cory Snyder)
* rgw: avoid string_view to temporary in RGWBulkUploadOp (`pr#52158 <https://github.com/ceph/ceph/pull/52158>`_, Casey Bodley)
* rgw: concurrency for multi object deletes (`pr#50208 <https://github.com/ceph/ceph/pull/50208>`_, Casey Bodley, Cory Snyder)
* rgw: D3N cache objects which oid contains slash (`pr#52320 <https://github.com/ceph/ceph/pull/52320>`_, Mark Kogan)
* rgw: fetch_remote_obj() preserves original part lengths for BlockDecrypt (`pr#52818 <https://github.com/ceph/ceph/pull/52818>`_, Casey Bodley)
* rgw: fix 2 null versionID after convert_plain_entry_to_versioned (`pr#53399 <https://github.com/ceph/ceph/pull/53399>`_, rui ma, zhuo li)
* rgw: fix consistency bug with OLH objects (`pr#52538 <https://github.com/ceph/ceph/pull/52538>`_, Cory Snyder)
* rgw: fix FP error when calculating enteries per bi shard (`pr#53592 <https://github.com/ceph/ceph/pull/53592>`_, J. Eric Ivancich)
* rgw: fix rgw rate limiting RGWRateLimitInfo class decode_json max_rea… (`pr#53766 <https://github.com/ceph/ceph/pull/53766>`_, xiangrui meng)
* rgw: fix SignatureDoesNotMatch when extra headers start with 'x-amz' (`pr#53771 <https://github.com/ceph/ceph/pull/53771>`_, rui ma)
* rgw: fix unwatch crash at radosgw startup (`pr#53761 <https://github.com/ceph/ceph/pull/53761>`_, lichaochao)
* rgw: handle http options CORS with v4 auth (`pr#53414 <https://github.com/ceph/ceph/pull/53414>`_, Tobias Urdin)
* rgw: improve buffer list utilization in the chunkupload scenario (`pr#53774 <https://github.com/ceph/ceph/pull/53774>`_, liubingrun)
* rgw: LDAP fix resource leak with wrong credentials (`pr#50562 <https://github.com/ceph/ceph/pull/50562>`_, Johannes Liebl, Johannes)
* rgw: optimizations for handling ECANCELED errors from within get_obj_state (`pr#50892 <https://github.com/ceph/ceph/pull/50892>`_, Cory Snyder)
* rgw: pick http_date in case of http_x_amz_date absence (`pr#53441 <https://github.com/ceph/ceph/pull/53441>`_, Seena Fallah, Mohamed Awnallah)
* rgw: retry metadata cache notifications with INVALIDATE_OBJ (`pr#52799 <https://github.com/ceph/ceph/pull/52799>`_, Casey Bodley)
* rgw: rgw_parse_url_bucket() rejects empty bucket names after 'tenant:' (`pr#50625 <https://github.com/ceph/ceph/pull/50625>`_, Casey Bodley)
* rgw: s3website doesn't prefetch for web_dir() check (`pr#53768 <https://github.com/ceph/ceph/pull/53768>`_, Casey Bodley)
* rgw: set keys from from master zone on admin api user create (`pr#51601 <https://github.com/ceph/ceph/pull/51601>`_, Ali Maredia)
* rgw: swift : check for valid key in POST forms (`pr#52739 <https://github.com/ceph/ceph/pull/52739>`_, Abhishek Lekshmanan)
* rgw: under fips & openssl 3.x allow md5 usage in select rgw ops (`pr#51269 <https://github.com/ceph/ceph/pull/51269>`_, Mark Kogan)
* rgwlc: prevent lc for one bucket from exceeding time budget (`pr#53561 <https://github.com/ceph/ceph/pull/53561>`_, Matt Benjamin)
* test/cli-integration/rbd: iSCSI REST API responses aren't pretty-printed anymore (`pr#52283 <https://github.com/ceph/ceph/pull/52283>`_, Ilya Dryomov)
* test: correct osd pool default size (`pr#51804 <https://github.com/ceph/ceph/pull/51804>`_, Nitzan Mordechai)
* test: monitor thrasher wait until quorum (`pr#51801 <https://github.com/ceph/ceph/pull/51801>`_, Nitzan Mordechai)
* tools/ceph-dencoder: Fix incorrect type define for trash_watcher (`pr#51779 <https://github.com/ceph/ceph/pull/51779>`_, Chen Yuanrun)
* tools/cephfs-data-scan: support for multi-datapool (`pr#50522 <https://github.com/ceph/ceph/pull/50522>`_, Mykola Golub)
* tools/cephfs: add basic detection/cleanup tool for dentry first damage (`pr#52245 <https://github.com/ceph/ceph/pull/52245>`_, Patrick Donnelly)
* tools/cephfs: include lost+found in scan_links (`pr#50783 <https://github.com/ceph/ceph/pull/50783>`_, Patrick Donnelly)
* vstart: check mgr status after starting mgr (`pr#51603 <https://github.com/ceph/ceph/pull/51603>`_, Rongqi Sun)
* vstart: fix text format (`pr#51124 <https://github.com/ceph/ceph/pull/51124>`_, Rongqi Sun)
* win32_deps_build: avoid pip (`pr#51129 <https://github.com/ceph/ceph/pull/51129>`_, Lucian Petrut, Ken Dreyer)
* Wip doc 2023 04 23 backport 51178 to quincy (`pr#51185 <https://github.com/ceph/ceph/pull/51185>`_, Zac Dover)
* Wip nitzan fixing few rados/test.sh (`pr#49938 <https://github.com/ceph/ceph/pull/49938>`_, Nitzan Mordechai)
* Wip nitzan pglog ec getattr error (`pr#49936 <https://github.com/ceph/ceph/pull/49936>`_, Nitzan Mordechai)

v17.2.6 Quincy
==============

This is the sixth backport release in the Quincy series. We recommend
that all users update to this release.

Notable Changes
---------------

* `ceph mgr dump` command now outputs `last_failure_osd_epoch` and
  `active_clients` fields at the top level.  Previously, these fields were
  output under `always_on_modules` field.

* telemetry: Added new metrics to the 'basic' channel to report per-pool bluestore
  compression metrics. See a sample report with `ceph telemetry preview`.
  Opt-in with `ceph telemetry on`.

Changelog
---------

* msg/async: don't abort when public addrs mismatch bind addrs (`pr#50575 <https://github.com/ceph/ceph/pull/50575>`_, Radoslaw Zarzynski)
* rgw: rgw_parse_url_bucket() rejects empty bucket names after 'tenant:' (`pr#50625 <https://github.com/ceph/ceph/pull/50625>`_, Casey Bodley)
* os/bluestore: Improve deferred write decision (`pr#49333 <https://github.com/ceph/ceph/pull/49333>`_, Adam Kupczyk, Igor Fedotov)
* rgw/cloud-transition: Fix issues with MCG endpoint (`pr#49061 <https://github.com/ceph/ceph/pull/49061>`_, Soumya Koduri)
* Add per OSD crush_device_class definition (`pr#50444 <https://github.com/ceph/ceph/pull/50444>`_, Francesco Pantano)
* ceph-crash: drop privileges to run as "ceph" user, rather than root (CVE-2022-3650) (`pr#48805 <https://github.com/ceph/ceph/pull/48805>`_, Tim Serong, Guillaume Abrioux)
* ceph-dencoder: Add erasure_code to denc-mod-osd's target_link_libraries (`pr#48028 <https://github.com/ceph/ceph/pull/48028>`_, Tim Serong)
* ceph-mixing: fix ceph_hosts variable (`pr#48934 <https://github.com/ceph/ceph/pull/48934>`_, Tatjana Dehler)
* ceph-volume/tests: add allowlist_externals to tox.ini (`pr#49788 <https://github.com/ceph/ceph/pull/49788>`_, Guillaume Abrioux)
* ceph-volume/tests: fix lvm centos8-filestore-create job (`pr#48122 <https://github.com/ceph/ceph/pull/48122>`_, Guillaume Abrioux)
* ceph-volume: add a retry in util.disk.remove_partition (`pr#47989 <https://github.com/ceph/ceph/pull/47989>`_, Guillaume Abrioux)
* ceph-volume: do not raise RuntimeError in util.lsblk (`pr#50144 <https://github.com/ceph/ceph/pull/50144>`_, Guillaume Abrioux)
* ceph-volume: fix a bug in get_all_devices_vgs() (`pr#49453 <https://github.com/ceph/ceph/pull/49453>`_, Guillaume Abrioux)
* ceph-volume: fix a bug in lsblk_all() (`pr#49868 <https://github.com/ceph/ceph/pull/49868>`_, Guillaume Abrioux)
* ceph-volume: legacy_encrypted() shouldn't call lsblk() when device is 'tmpfs' (`pr#50161 <https://github.com/ceph/ceph/pull/50161>`_, Guillaume Abrioux)
* ceph.spec.in: disable system_pmdk on s390x for SUSE distros (`pr#48522 <https://github.com/ceph/ceph/pull/48522>`_, Tim Serong)
* ceph.spec.in: Replace %usrmerged macro with regular version check (`pr#49831 <https://github.com/ceph/ceph/pull/49831>`_, Tim Serong)
* ceph.spec.in: Use gcc11-c++ on openSUSE Leap 15.x (`pr#48058 <https://github.com/ceph/ceph/pull/48058>`_, Tim Serong)
* ceph_fuse: retry the test_dentry_handling if fails (`pr#49942 <https://github.com/ceph/ceph/pull/49942>`_, Xiubo Li)
* cephadm: add `ip_nonlocal_bind` to haproxy deployment (`pr#48211 <https://github.com/ceph/ceph/pull/48211>`_, Michael Fritch)
* cephadm: Adding poststop actions and setting TimeoutStartSec to 200s (`pr#50447 <https://github.com/ceph/ceph/pull/50447>`_, Redouane Kachach)
* cephadm: consider stdout to get container version (`pr#48208 <https://github.com/ceph/ceph/pull/48208>`_, Tatjana Dehler)
* cephadm: don't overwrite cluster logrotate file (`pr#49849 <https://github.com/ceph/ceph/pull/49849>`_, Adam King)
* cephadm: Fix disk size calculation (`pr#47945 <https://github.com/ceph/ceph/pull/47945>`_, Paul Cuzner)
* cephadm: only pull host info from applied spec, don't try to parse yaml (`pr#49854 <https://github.com/ceph/ceph/pull/49854>`_, Adam King)
* cephadm: pin flake8 to 5.0.4 (`pr#49059 <https://github.com/ceph/ceph/pull/49059>`_, Kefu Chai)
* cephadm: run tests as root (`pr#48434 <https://github.com/ceph/ceph/pull/48434>`_, Kefu Chai)
* cephadm: set pids-limit unlimited for all ceph daemons (`pr#50448 <https://github.com/ceph/ceph/pull/50448>`_, Adam King, Teoman ONAY)
* cephadm: support quotes around public/cluster network in config passed to bootstrap (`pr#47660 <https://github.com/ceph/ceph/pull/47660>`_, Adam King)
* cephadm: using short hostname to create the initial mon and mgr (`pr#50445 <https://github.com/ceph/ceph/pull/50445>`_, Redouane Kachach)
* cephfs-data-scan: make scan_links more verbose (`pr#48442 <https://github.com/ceph/ceph/pull/48442>`_, Mykola Golub)
* cephfs-top, mgr/stats: multiple file system support with UI (`pr#47820 <https://github.com/ceph/ceph/pull/47820>`_, Neeraj Pratap Singh)
* cephfs-top: addition of sort feature and limit option (`pr#50151 <https://github.com/ceph/ceph/pull/50151>`_, Neeraj Pratap Singh, Jos Collin)
* cephfs-top: make cephfs-top display scrollable (`pr#48677 <https://github.com/ceph/ceph/pull/48677>`_, Jos Collin)
* client: abort the client if we couldn't invalidate dentry caches (`pr#48110 <https://github.com/ceph/ceph/pull/48110>`_, Xiubo Li)
* client: do not uninline data for read (`pr#48132 <https://github.com/ceph/ceph/pull/48132>`_, Xiubo Li)
* client: fix incorrectly showing the .snap size for stat (`pr#48414 <https://github.com/ceph/ceph/pull/48414>`_, Xiubo Li)
* client: stop the remount_finisher thread in the Client::unmount() (`pr#48107 <https://github.com/ceph/ceph/pull/48107>`_, Xiubo Li)
* client: use parent directory POSIX ACLs for snapshot dir (`issue#57084 <http://tracker.ceph.com/issues/57084>`_, `pr#48563 <https://github.com/ceph/ceph/pull/48563>`_, Venky Shankar)
* cls/queue: use larger read chunks in queue_list_entries (`pr#49902 <https://github.com/ceph/ceph/pull/49902>`_, Igor Fedotov)
* cls/rbd: update last_read in group::snap_list (`pr#49196 <https://github.com/ceph/ceph/pull/49196>`_, Ilya Dryomov, Prasanna Kumar Kalever)
* cls/rgw: remove index entry after cancelling last racing delete op (`pr#50241 <https://github.com/ceph/ceph/pull/50241>`_, Casey Bodley)
* cmake: bump node version to 14 (`pr#50231 <https://github.com/ceph/ceph/pull/50231>`_, Nizamudeen A)
* cmake: re-enable TCMalloc and allocator related cleanups (`pr#47927 <https://github.com/ceph/ceph/pull/47927>`_, Kefu Chai)
* CODEOWNERS: assign qa/workunits/windows to RBD (`pr#50304 <https://github.com/ceph/ceph/pull/50304>`_, Ilya Dryomov)
* common/ceph_context: leak some memory fail to show in valgrind (`pr#47933 <https://github.com/ceph/ceph/pull/47933>`_, Nitzan Mordechai)
* common: fix build with GCC 13 (missing <cstdint> include) (`pr#48719 <https://github.com/ceph/ceph/pull/48719>`_, Sam James)
* common: notify all when max backlog reached in OutputDataSocket (`pr#47233 <https://github.com/ceph/ceph/pull/47233>`_, Shu Yu)
* compressor: fix rpmbuild on RHEL-8 (`pr#48314 <https://github.com/ceph/ceph/pull/48314>`_, Andriy Tkachuk)
* doc/_static: add scroll-margin-top to custom.css (`pr#49644 <https://github.com/ceph/ceph/pull/49644>`_, Zac Dover)
* doc/architecture: correct PDF link (`pr#48795 <https://github.com/ceph/ceph/pull/48795>`_, Zac Dover)
* doc/ceph-volume: add A. D'Atri's suggestions (`pr#48645 <https://github.com/ceph/ceph/pull/48645>`_, Zac Dover)
* doc/ceph-volume: fix cephadm references (`pr#50115 <https://github.com/ceph/ceph/pull/50115>`_, Piotr Parczewski)
* doc/ceph-volume: improve prepare.rst (`pr#48668 <https://github.com/ceph/ceph/pull/48668>`_, Zac Dover)
* doc/ceph-volume: refine "bluestore" section (`pr#48634 <https://github.com/ceph/ceph/pull/48634>`_, Zac Dover)
* doc/ceph-volume: refine "filestore" section (`pr#48636 <https://github.com/ceph/ceph/pull/48636>`_, Zac Dover)
* doc/ceph-volume: refine "prepare" top matter (`pr#48651 <https://github.com/ceph/ceph/pull/48651>`_, Zac Dover)
* doc/ceph-volume: refine encryption.rst (`pr#49792 <https://github.com/ceph/ceph/pull/49792>`_, Zac Dover)
* doc/ceph-volume: refine Filestore docs (`pr#48670 <https://github.com/ceph/ceph/pull/48670>`_, Zac Dover)
* doc/ceph-volume: update LUKS docs (`pr#49757 <https://github.com/ceph/ceph/pull/49757>`_, Zac Dover)
* doc/cephadm - remove "danger" admonition (`pr#49169 <https://github.com/ceph/ceph/pull/49169>`_, Zac Dover)
* doc/cephadm/host-management: add service spec link (`pr#50254 <https://github.com/ceph/ceph/pull/50254>`_, thomas)
* doc/cephadm/troubleshooting: remove word repeat (`pr#50222 <https://github.com/ceph/ceph/pull/50222>`_, thomas)
* doc/cephadm: add airgapped install procedure (`pr#49145 <https://github.com/ceph/ceph/pull/49145>`_, Zac Dover)
* doc/cephadm: add info about --no-overwrite to note about tuned-profiles (`pr#47954 <https://github.com/ceph/ceph/pull/47954>`_, Adam King)
* doc/cephadm: add prompts to host-management.rst (`pr#48589 <https://github.com/ceph/ceph/pull/48589>`_, Zac Dover)
* doc/cephadm: alphabetize external tools list (`pr#48725 <https://github.com/ceph/ceph/pull/48725>`_, Zac Dover)
* doc/cephadm: arrange "listing hosts" section (`pr#48723 <https://github.com/ceph/ceph/pull/48723>`_, Zac Dover)
* doc/cephadm: clean colons in host-management.rst (`pr#48603 <https://github.com/ceph/ceph/pull/48603>`_, Zac Dover)
* doc/cephadm: correct version staggered upgrade got in pacific (`pr#48055 <https://github.com/ceph/ceph/pull/48055>`_, Adam King)
* doc/cephadm: document recommended syntax for mounting files with ECA (`pr#48068 <https://github.com/ceph/ceph/pull/48068>`_, Adam King)
* doc/cephadm: fix grammar in compatibility.rst (`pr#48714 <https://github.com/ceph/ceph/pull/48714>`_, Zac Dover)
* doc/cephadm: fix tuned-profile add/rm-setting syntax example (`pr#48094 <https://github.com/ceph/ceph/pull/48094>`_, Adam King)
* doc/cephadm: format airgap install procedure (`pr#49148 <https://github.com/ceph/ceph/pull/49148>`_, Zac Dover)
* doc/cephadm: grammar / syntax in install.rst (`pr#49948 <https://github.com/ceph/ceph/pull/49948>`_, Piotr Parczewski)
* doc/cephadm: improve airgapping procedure grammar (`pr#49157 <https://github.com/ceph/ceph/pull/49157>`_, Zac Dover)
* doc/cephadm: improve front matter (`pr#48606 <https://github.com/ceph/ceph/pull/48606>`_, Zac Dover)
* doc/cephadm: improve grammar in "listing hosts" (`pr#49164 <https://github.com/ceph/ceph/pull/49164>`_, Zac Dover)
* doc/cephadm: improve lone sentence (`pr#48737 <https://github.com/ceph/ceph/pull/48737>`_, Zac Dover)
* doc/cephadm: Redd up compatibility.rst (`pr#50367 <https://github.com/ceph/ceph/pull/50367>`_, Anthony D'Atri)
* doc/cephadm: refine "os tuning" in h. management (`pr#48573 <https://github.com/ceph/ceph/pull/48573>`_, Zac Dover)
* doc/cephadm: refine "Removing Hosts" (`pr#49706 <https://github.com/ceph/ceph/pull/49706>`_, Zac Dover)
* doc/cephadm: s/osd/OSD/ where appropriate (`pr#49717 <https://github.com/ceph/ceph/pull/49717>`_, Zac Dover)
* doc/cephadm: s/ssh/SSH/ in doc/cephadm (complete) (`pr#48611 <https://github.com/ceph/ceph/pull/48611>`_, Zac Dover)
* doc/cephadm: s/ssh/SSH/ in troubleshooting.rst (`pr#48601 <https://github.com/ceph/ceph/pull/48601>`_, Zac Dover)
* doc/cephadm: update cephadm compatability and stability page (`pr#50336 <https://github.com/ceph/ceph/pull/50336>`_, Adam King)
* doc/cephadm: update install.rst (`pr#48594 <https://github.com/ceph/ceph/pull/48594>`_, Zac Dover)
* doc/cephfs - s/yet to here/yet to hear/ posix.rst (`pr#49448 <https://github.com/ceph/ceph/pull/49448>`_, Zac Dover)
* doc/cephfs: add note about CephFS extended attributes and getfattr (`pr#50068 <https://github.com/ceph/ceph/pull/50068>`_, Zac Dover)
* doc/cephfs: describe conf opt "client quota df" in quota doc (`pr#50252 <https://github.com/ceph/ceph/pull/50252>`_, Rishabh Dave)
* doc/cephfs: fix "e.g." in posix.rst (`pr#49450 <https://github.com/ceph/ceph/pull/49450>`_, Zac Dover)
* doc/cephfs: s/all of there are/all of these are/ (`pr#49446 <https://github.com/ceph/ceph/pull/49446>`_, Zac Dover)
* doc/css: add "span" padding to custom.css (`pr#49693 <https://github.com/ceph/ceph/pull/49693>`_, Zac Dover)
* doc/css: add scroll-margin-top to dt elements (`pr#49639 <https://github.com/ceph/ceph/pull/49639>`_, Zac Dover)
* doc/css: Add scroll-margin-top to h2 html element (`pr#49661 <https://github.com/ceph/ceph/pull/49661>`_, Zac Dover)
* doc/css: add top-bar padding for h3 html element (`pr#49701 <https://github.com/ceph/ceph/pull/49701>`_, Zac Dover)
* doc/dev/cephadm: fix host maintenance enter/exit syntax (`pr#49646 <https://github.com/ceph/ceph/pull/49646>`_, Ranjini Mandyam Narasiodeyar)
* doc/dev/developer_guide/testing_integration_tests: Add Upgrade Testin… (`pr#49909 <https://github.com/ceph/ceph/pull/49909>`_, Matan Breizman)
* doc/dev/developer_guide/tests-unit-tests: Add unit test caveat (`pr#49012 <https://github.com/ceph/ceph/pull/49012>`_, Matan Breizman)
* doc/dev: add explanation of how to use deduplication (`pr#48567 <https://github.com/ceph/ceph/pull/48567>`_, Myoungwon Oh)
* doc/dev: add full stop to sentence in basic-wo (`pr#50400 <https://github.com/ceph/ceph/pull/50400>`_, Zac Dover)
* doc/dev: add git branch management commands (`pr#49738 <https://github.com/ceph/ceph/pull/49738>`_, Zac Dover)
* doc/dev: add Slack to Dev Guide essentials (`pr#49874 <https://github.com/ceph/ceph/pull/49874>`_, Zac Dover)
* doc/dev: add submodule-update link to dev guide (`pr#48479 <https://github.com/ceph/ceph/pull/48479>`_, Zac Dover)
* doc/dev: alphabetize EC glossary (`pr#48685 <https://github.com/ceph/ceph/pull/48685>`_, Zac Dover)
* doc/dev: fix graphviz diagram (`pr#48922 <https://github.com/ceph/ceph/pull/48922>`_, Zac Dover)
* doc/dev: improve Basic Workflow wording (`pr#49077 <https://github.com/ceph/ceph/pull/49077>`_, Zac Dover)
* doc/dev: improve EC glossary (`pr#48675 <https://github.com/ceph/ceph/pull/48675>`_, Zac Dover)
* doc/dev: improve lone sentence (`pr#48740 <https://github.com/ceph/ceph/pull/48740>`_, Zac Dover)
* doc/dev: improve presentation of note (git remote) (`pr#48237 <https://github.com/ceph/ceph/pull/48237>`_, Zac Dover)
* doc/dev: link to Dot User's Manual (`pr#48925 <https://github.com/ceph/ceph/pull/48925>`_, Zac Dover)
* doc/dev: refine erasure_coding.rst (`pr#48700 <https://github.com/ceph/ceph/pull/48700>`_, Zac Dover)
* doc/dev: remove deduplication.rst from quincy (`pr#48570 <https://github.com/ceph/ceph/pull/48570>`_, Zac Dover)
* doc/dev: use underscores in config vars (`pr#49892 <https://github.com/ceph/ceph/pull/49892>`_, Ville Ojamo)
* doc/glosary.rst: add "Ceph Block Device" term (`pr#48746 <https://github.com/ceph/ceph/pull/48746>`_, Zac Dover)
* doc/glossary - add "secrets" (`pr#49397 <https://github.com/ceph/ceph/pull/49397>`_, Zac Dover)
* doc/glossary.rst: add "Ceph Dashboard" term (`pr#48748 <https://github.com/ceph/ceph/pull/48748>`_, Zac Dover)
* doc/glossary.rst: alphabetize glossary terms (`pr#48338 <https://github.com/ceph/ceph/pull/48338>`_, Zac Dover)
* doc/glossary.rst: define "Ceph Manager" (`pr#48764 <https://github.com/ceph/ceph/pull/48764>`_, Zac Dover)
* doc/glossary.rst: remove duplicates (`pr#48357 <https://github.com/ceph/ceph/pull/48357>`_, Zac Dover)
* doc/glossary.rst: remove old front matter (`pr#48754 <https://github.com/ceph/ceph/pull/48754>`_, Zac Dover)
* doc/glossary: add "application" to the glossary (`pr#50258 <https://github.com/ceph/ceph/pull/50258>`_, Zac Dover)
* doc/glossary: add "BlueStore" (`pr#48777 <https://github.com/ceph/ceph/pull/48777>`_, Zac Dover)
* doc/glossary: add "Bucket" (`pr#50224 <https://github.com/ceph/ceph/pull/50224>`_, Zac Dover)
* doc/glossary: add "ceph monitor" entry (`pr#48447 <https://github.com/ceph/ceph/pull/48447>`_, Zac Dover)
* doc/glossary: add "Ceph Object Store" (`pr#49030 <https://github.com/ceph/ceph/pull/49030>`_, Zac Dover)
* doc/glossary: add "client" to glossary (`pr#50262 <https://github.com/ceph/ceph/pull/50262>`_, Zac Dover)
* doc/glossary: add "Dashboard Module" (`pr#49137 <https://github.com/ceph/ceph/pull/49137>`_, Zac Dover)
* doc/glossary: add "FQDN" entry (`pr#49424 <https://github.com/ceph/ceph/pull/49424>`_, Zac Dover)
* doc/glossary: add "mds" term (`pr#48871 <https://github.com/ceph/ceph/pull/48871>`_, Zac Dover)
* doc/glossary: add "Period" to glossary (`pr#50155 <https://github.com/ceph/ceph/pull/50155>`_, Zac Dover)
* doc/glossary: add "RADOS Cluster" (`pr#49134 <https://github.com/ceph/ceph/pull/49134>`_, Zac Dover)
* doc/glossary: add "RADOS" definition (`pr#48950 <https://github.com/ceph/ceph/pull/48950>`_, Zac Dover)
* doc/glossary: add "realm" to glossary (`pr#50134 <https://github.com/ceph/ceph/pull/50134>`_, Zac Dover)
* doc/glossary: Add "zone" to glossary.rst (`pr#50271 <https://github.com/ceph/ceph/pull/50271>`_, Zac Dover)
* doc/glossary: add AWS/OpenStack bucket info (`pr#50247 <https://github.com/ceph/ceph/pull/50247>`_, Zac Dover)
* doc/glossary: add DAS (`pr#49254 <https://github.com/ceph/ceph/pull/49254>`_, Zac Dover)
* doc/glossary: add matter to "RBD" (`pr#49265 <https://github.com/ceph/ceph/pull/49265>`_, Zac Dover)
* doc/glossary: add oxford comma to "Cluster Map" (`pr#48992 <https://github.com/ceph/ceph/pull/48992>`_, Zac Dover)
* doc/glossary: beef up "Ceph Block Storage" (`pr#48964 <https://github.com/ceph/ceph/pull/48964>`_, Zac Dover)
* doc/glossary: capitalize "DAS" correctly (`pr#49603 <https://github.com/ceph/ceph/pull/49603>`_, Zac Dover)
* doc/glossary: clean OSD id-related entries (`pr#49589 <https://github.com/ceph/ceph/pull/49589>`_, Zac Dover)
* doc/glossary: Clean up "Ceph Object Storage" (`pr#49667 <https://github.com/ceph/ceph/pull/49667>`_, Zac Dover)
* doc/glossary: collate "releases" entries (`pr#49600 <https://github.com/ceph/ceph/pull/49600>`_, Zac Dover)
* doc/glossary: Define "Ceph Node" (`pr#48994 <https://github.com/ceph/ceph/pull/48994>`_, Zac Dover)
* doc/glossary: define "Ceph Object Gateway" (`pr#48901 <https://github.com/ceph/ceph/pull/48901>`_, Zac Dover)
* doc/glossary: define "Ceph OSD" (`pr#48770 <https://github.com/ceph/ceph/pull/48770>`_, Zac Dover)
* doc/glossary: define "Ceph Storage Cluster" (`pr#49002 <https://github.com/ceph/ceph/pull/49002>`_, Zac Dover)
* doc/glossary: define "OSD" (`pr#48759 <https://github.com/ceph/ceph/pull/48759>`_, Zac Dover)
* doc/glossary: define "RGW" (`pr#48960 <https://github.com/ceph/ceph/pull/48960>`_, Zac Dover)
* doc/glossary: disambiguate "OSD" (`pr#48790 <https://github.com/ceph/ceph/pull/48790>`_, Zac Dover)
* doc/glossary: disambiguate clauses (`pr#49574 <https://github.com/ceph/ceph/pull/49574>`_, Zac Dover)
* doc/glossary: fix "Ceph Client" (`pr#49032 <https://github.com/ceph/ceph/pull/49032>`_, Zac Dover)
* doc/glossary: improve "Ceph Manager Dashboard" (`pr#48824 <https://github.com/ceph/ceph/pull/48824>`_, Zac Dover)
* doc/glossary: improve "Ceph Manager" term (`pr#48811 <https://github.com/ceph/ceph/pull/48811>`_, Zac Dover)
* doc/glossary: improve "Ceph Point Release" entry (`pr#48890 <https://github.com/ceph/ceph/pull/48890>`_, Zac Dover)
* doc/glossary: improve "ceph" term (`pr#48820 <https://github.com/ceph/ceph/pull/48820>`_, Zac Dover)
* doc/glossary: improve wording (`pr#48751 <https://github.com/ceph/ceph/pull/48751>`_, Zac Dover)
* doc/glossary: link to "Ceph Manager" (`pr#49063 <https://github.com/ceph/ceph/pull/49063>`_, Zac Dover)
* doc/glossary: link to OSD material (`pr#48779 <https://github.com/ceph/ceph/pull/48779>`_, zdover23, Zac Dover)
* doc/glossary: redirect entries to "Ceph OSD" (`pr#48833 <https://github.com/ceph/ceph/pull/48833>`_, Zac Dover)
* doc/glossary: remove "Ceph System" (`pr#49072 <https://github.com/ceph/ceph/pull/49072>`_, Zac Dover)
* doc/glossary: remove "Ceph Test Framework" (`pr#48841 <https://github.com/ceph/ceph/pull/48841>`_, Zac Dover)
* doc/glossary: rewrite "Ceph File System" (`pr#48917 <https://github.com/ceph/ceph/pull/48917>`_, Zac Dover)
* doc/glossary: s/an/each/ where it's needed (`pr#49595 <https://github.com/ceph/ceph/pull/49595>`_, Zac Dover)
* doc/glossary: s/Ceph System/Ceph Cluster/ (`pr#49080 <https://github.com/ceph/ceph/pull/49080>`_, Zac Dover)
* doc/glossary: s/comprising/consisting of/ (`pr#49018 <https://github.com/ceph/ceph/pull/49018>`_, Zac Dover)
* doc/glossary: update "Cluster Map" (`pr#48797 <https://github.com/ceph/ceph/pull/48797>`_, Zac Dover)
* doc/glossary: update "pool/pools" (`pr#48857 <https://github.com/ceph/ceph/pull/48857>`_, Zac Dover)
* doc/index: remove "uniquely" from landing page (`pr#50477 <https://github.com/ceph/ceph/pull/50477>`_, Zac Dover)
* doc/install: clone-source.rst s/master/main (`pr#48380 <https://github.com/ceph/ceph/pull/48380>`_, Zac Dover)
* doc/install: improve updating submodules procedure (`pr#48464 <https://github.com/ceph/ceph/pull/48464>`_, Zac Dover)
* doc/install: link to "cephadm installing ceph" (`pr#49781 <https://github.com/ceph/ceph/pull/49781>`_, Zac Dover)
* doc/install: refine index.rst (`pr#50435 <https://github.com/ceph/ceph/pull/50435>`_, Zac Dover)
* doc/install: update "Official Releases" sources (`pr#49038 <https://github.com/ceph/ceph/pull/49038>`_, Zac Dover)
* doc/install: update clone-source.rst (`pr#49377 <https://github.com/ceph/ceph/pull/49377>`_, Zac Dover)
* doc/install: update index.rst (`pr#50432 <https://github.com/ceph/ceph/pull/50432>`_, Zac Dover)
* doc/man/ceph-rbdnamer: remove obsolete udev rule (`pr#49697 <https://github.com/ceph/ceph/pull/49697>`_, Ilya Dryomov)
* doc/man: define --num-rep, --min-rep and --max-rep (`pr#49659 <https://github.com/ceph/ceph/pull/49659>`_, Zac Dover)
* doc/man: disambiguate "user" in a command (`pr#48954 <https://github.com/ceph/ceph/pull/48954>`_, Zac Dover)
* doc/mgr: name data source in "Man Install & Config" (`pr#48370 <https://github.com/ceph/ceph/pull/48370>`_, Zac Dover)
* doc/monitoring: add min vers of apps in mon stack (`pr#48063 <https://github.com/ceph/ceph/pull/48063>`_, Zac Dover, Himadri Maheshwari)
* doc/osd: Fixes the introduction for writeback mode of cache tier (`pr#48882 <https://github.com/ceph/ceph/pull/48882>`_, Mingyuan Liang)
* doc/rados/operations: Fix double prompt (`pr#49898 <https://github.com/ceph/ceph/pull/49898>`_, Ville Ojamo)
* doc/rados/operations: Fix indentation (`pr#49895 <https://github.com/ceph/ceph/pull/49895>`_, Ville Ojamo)
* doc/rados/operations: Improve wording, capitalization, formatting (`pr#50453 <https://github.com/ceph/ceph/pull/50453>`_, Anthony D'Atri)
* doc/rados: add prompts to add-or-remove-osds (`pr#49070 <https://github.com/ceph/ceph/pull/49070>`_, Zac Dover)
* doc/rados: add prompts to add-or-rm-prompts.rst (`pr#48985 <https://github.com/ceph/ceph/pull/48985>`_, Zac Dover)
* doc/rados: add prompts to add-or-rm-prompts.rst (`pr#48979 <https://github.com/ceph/ceph/pull/48979>`_, Zac Dover)
* doc/rados: add prompts to auth-config-ref.rst (`pr#49515 <https://github.com/ceph/ceph/pull/49515>`_, Zac Dover)
* doc/rados: add prompts to balancer.rst (`pr#49111 <https://github.com/ceph/ceph/pull/49111>`_, Zac Dover)
* doc/rados: add prompts to bluestore-config-ref.rst (`pr#49535 <https://github.com/ceph/ceph/pull/49535>`_, Zac Dover)
* doc/rados: add prompts to bluestore-migration.rst (`pr#49122 <https://github.com/ceph/ceph/pull/49122>`_, Zac Dover)
* doc/rados: add prompts to cache-tiering.rst (`pr#49124 <https://github.com/ceph/ceph/pull/49124>`_, Zac Dover)
* doc/rados: add prompts to ceph-conf.rst (`pr#49492 <https://github.com/ceph/ceph/pull/49492>`_, Zac Dover)
* doc/rados: add prompts to change-mon-elections.rst (`pr#49129 <https://github.com/ceph/ceph/pull/49129>`_, Zac Dover)
* doc/rados: add prompts to control.rst (`pr#49126 <https://github.com/ceph/ceph/pull/49126>`_, Zac Dover)
* doc/rados: add prompts to crush-map.rst (`pr#49183 <https://github.com/ceph/ceph/pull/49183>`_, Zac Dover)
* doc/rados: add prompts to devices.rst (`pr#49187 <https://github.com/ceph/ceph/pull/49187>`_, Zac Dover)
* doc/rados: add prompts to erasure-code-clay.rst (`pr#49205 <https://github.com/ceph/ceph/pull/49205>`_, Zac Dover)
* doc/rados: add prompts to erasure-code-isa (`pr#49207 <https://github.com/ceph/ceph/pull/49207>`_, Zac Dover)
* doc/rados: add prompts to erasure-code-jerasure.rst (`pr#49209 <https://github.com/ceph/ceph/pull/49209>`_, Zac Dover)
* doc/rados: add prompts to erasure-code-lrc.rst (`pr#49218 <https://github.com/ceph/ceph/pull/49218>`_, Zac Dover)
* doc/rados: add prompts to erasure-code-shec.rst (`pr#49220 <https://github.com/ceph/ceph/pull/49220>`_, Zac Dover)
* doc/rados: add prompts to health-checks (1 of 5) (`pr#49222 <https://github.com/ceph/ceph/pull/49222>`_, Zac Dover)
* doc/rados: add prompts to health-checks (2 of 5) (`pr#49224 <https://github.com/ceph/ceph/pull/49224>`_, Zac Dover)
* doc/rados: add prompts to health-checks (3 of 5) (`pr#49226 <https://github.com/ceph/ceph/pull/49226>`_, Zac Dover)
* doc/rados: add prompts to health-checks (4 of 5) (`pr#49228 <https://github.com/ceph/ceph/pull/49228>`_, Zac Dover)
* doc/rados: add prompts to health-checks (5 of 5) (`pr#49230 <https://github.com/ceph/ceph/pull/49230>`_, Zac Dover)
* doc/rados: add prompts to librados-intro.rst (`pr#49551 <https://github.com/ceph/ceph/pull/49551>`_, Zac Dover)
* doc/rados: add prompts to monitoring-osd-pg.rst (`pr#49239 <https://github.com/ceph/ceph/pull/49239>`_, Zac Dover)
* doc/rados: add prompts to monitoring.rst (`pr#49244 <https://github.com/ceph/ceph/pull/49244>`_, Zac Dover)
* doc/rados: add prompts to msgr2.rst (`pr#49511 <https://github.com/ceph/ceph/pull/49511>`_, Zac Dover)
* doc/rados: add prompts to pg-repair.rst (`pr#49246 <https://github.com/ceph/ceph/pull/49246>`_, Zac Dover)
* doc/rados: add prompts to placement-groups.rst (`pr#49273 <https://github.com/ceph/ceph/pull/49273>`_, Zac Dover)
* doc/rados: add prompts to placement-groups.rst (`pr#49271 <https://github.com/ceph/ceph/pull/49271>`_, Zac Dover)
* doc/rados: add prompts to placement-groups.rst (3) (`pr#49275 <https://github.com/ceph/ceph/pull/49275>`_, Zac Dover)
* doc/rados: add prompts to pools.rst (`pr#48061 <https://github.com/ceph/ceph/pull/48061>`_, Zac Dover)
* doc/rados: add prompts to stretch-mode.rst (`pr#49369 <https://github.com/ceph/ceph/pull/49369>`_, Zac Dover)
* doc/rados: add prompts to upmap.rst (`pr#49371 <https://github.com/ceph/ceph/pull/49371>`_, Zac Dover)
* doc/rados: add prompts to user-management.rst (`pr#49384 <https://github.com/ceph/ceph/pull/49384>`_, Zac Dover)
* doc/rados: clarify default EC pool from simplest (`pr#49468 <https://github.com/ceph/ceph/pull/49468>`_, Zac Dover)
* doc/rados: cleanup "erasure code profiles" (`pr#49050 <https://github.com/ceph/ceph/pull/49050>`_, Zac Dover)
* doc/rados: correct typo in python.rst (`pr#49559 <https://github.com/ceph/ceph/pull/49559>`_, Zac Dover)
* doc/rados: fix grammar in configuration/index.rst (`pr#48884 <https://github.com/ceph/ceph/pull/48884>`_, Zac Dover)
* doc/rados: fix prompts in erasure-code.rst (`pr#48334 <https://github.com/ceph/ceph/pull/48334>`_, Zac Dover)
* doc/rados: improve pools.rst (`pr#48867 <https://github.com/ceph/ceph/pull/48867>`_, Zac Dover)
* doc/rados: link to cephadm replacing osd section (`pr#49680 <https://github.com/ceph/ceph/pull/49680>`_, Zac Dover)
* doc/rados: move colon (`pr#49704 <https://github.com/ceph/ceph/pull/49704>`_, Zac Dover)
* doc/rados: refine ceph-conf.rst (`pr#49832 <https://github.com/ceph/ceph/pull/49832>`_, Zac Dover)
* doc/rados: refine English in crush-map-edits.rst (`pr#48365 <https://github.com/ceph/ceph/pull/48365>`_, Zac Dover)
* doc/rados: refine pool-pg-config-ref.rst (`pr#49821 <https://github.com/ceph/ceph/pull/49821>`_, Zac Dover)
* doc/rados: remove prompt from php.ini line (`pr#49561 <https://github.com/ceph/ceph/pull/49561>`_, Zac Dover)
* doc/rados: reword part of cache-tiering.rst (`pr#48887 <https://github.com/ceph/ceph/pull/48887>`_, Zac Dover)
* doc/rados: rewrite EC intro (`pr#48323 <https://github.com/ceph/ceph/pull/48323>`_, Zac Dover)
* doc/rados: s/backend/back end/ (`pr#48781 <https://github.com/ceph/ceph/pull/48781>`_, Zac Dover)
* doc/rados: update "Pools" material (`pr#48855 <https://github.com/ceph/ceph/pull/48855>`_, Zac Dover)
* doc/rados: update OSD_BACKFILLFULL description (`pr#50218 <https://github.com/ceph/ceph/pull/50218>`_, Ponnuvel Palaniyappan)
* doc/rados: update prompts in crush-map-edits.rst (`pr#48363 <https://github.com/ceph/ceph/pull/48363>`_, Zac Dover)
* doc/rados: update prompts in network-config-ref (`pr#48159 <https://github.com/ceph/ceph/pull/48159>`_, Zac Dover)
* doc/radosgw/STS: sts_key and user capabilities (`pr#47324 <https://github.com/ceph/ceph/pull/47324>`_, Tobias Bossert)
* doc/radosgw: add prompts to multisite.rst (`pr#48659 <https://github.com/ceph/ceph/pull/48659>`_, Zac Dover)
* doc/radosgw: add push_endpoint for rabbitmq (`pr#48487 <https://github.com/ceph/ceph/pull/48487>`_, Zac Dover)
* doc/radosgw: format admonitions (`pr#50356 <https://github.com/ceph/ceph/pull/50356>`_, Zac Dover)
* doc/radosgw: improve "Ceph Object Gateway" text (`pr#48863 <https://github.com/ceph/ceph/pull/48863>`_, Zac Dover)
* doc/radosgw: improve grammar - notifications.rst (`pr#48494 <https://github.com/ceph/ceph/pull/48494>`_, Zac Dover)
* doc/radosgw: multisite - edit "functional changes" (`pr#50277 <https://github.com/ceph/ceph/pull/50277>`_, Zac Dover)
* doc/radosgw: refine "bucket notifications" (`pr#48560 <https://github.com/ceph/ceph/pull/48560>`_, Zac Dover)
* doc/radosgw: refine "Maintenance" in multisite.rst (`pr#50025 <https://github.com/ceph/ceph/pull/50025>`_, Zac Dover)
* doc/radosgw: refine "notification reliability" (`pr#48529 <https://github.com/ceph/ceph/pull/48529>`_, Zac Dover)
* doc/radosgw: refine "notifications" and "events" (`pr#48579 <https://github.com/ceph/ceph/pull/48579>`_, Zac Dover)
* doc/radosgw: refine notifications.rst - top part (`pr#48502 <https://github.com/ceph/ceph/pull/48502>`_, Zac Dover)
* doc/radosgw: s/execute/run/ in multisite.rst (`pr#50173 <https://github.com/ceph/ceph/pull/50173>`_, Zac Dover)
* doc/radosgw: s/zone group/zonegroup/g et alia (`pr#50297 <https://github.com/ceph/ceph/pull/50297>`_, Zac Dover)
* doc/radosgw: update notifications.rst - grammar (`pr#48499 <https://github.com/ceph/ceph/pull/48499>`_, Zac Dover)
* doc/radosw: improve radosgw text (`pr#48966 <https://github.com/ceph/ceph/pull/48966>`_, Zac Dover)
* doc/radowsgw: add prompts to notifications.rst (`pr#48535 <https://github.com/ceph/ceph/pull/48535>`_, Zac Dover)
* doc/rbd/rbd-exclusive-locks: warn about automatic lock transitions (`pr#49806 <https://github.com/ceph/ceph/pull/49806>`_, Ilya Dryomov)
* doc/rbd: format iscsi-initiator-linux.rbd better (`pr#49749 <https://github.com/ceph/ceph/pull/49749>`_, Zac Dover)
* doc/rbd: improve grammar in "immutable object..." (`pr#48969 <https://github.com/ceph/ceph/pull/48969>`_, Zac Dover)
* doc/rbd: refine "Create a Block Device Pool" (`pr#49307 <https://github.com/ceph/ceph/pull/49307>`_, Zac Dover)
* doc/rbd: refine "Create a Block Device User" (`pr#49318 <https://github.com/ceph/ceph/pull/49318>`_, Zac Dover)
* doc/rbd: refine "Create a Block Device User" (`pr#49300 <https://github.com/ceph/ceph/pull/49300>`_, Zac Dover)
* doc/rbd: refine "Creating a Block Device Image" (`pr#49346 <https://github.com/ceph/ceph/pull/49346>`_, Zac Dover)
* doc/rbd: refine "Listing Block Device Images" (`pr#49348 <https://github.com/ceph/ceph/pull/49348>`_, Zac Dover)
* doc/rbd: refine "Removing a Block Device Image" (`pr#49356 <https://github.com/ceph/ceph/pull/49356>`_, Zac Dover)
* doc/rbd: refine "Resizing a Block Device Image" (`pr#49352 <https://github.com/ceph/ceph/pull/49352>`_, Zac Dover)
* doc/rbd: refine "Restoring a Block Device Image" (`pr#49354 <https://github.com/ceph/ceph/pull/49354>`_, Zac Dover)
* doc/rbd: refine "Retrieving Image Information" (`pr#49350 <https://github.com/ceph/ceph/pull/49350>`_, Zac Dover)
* doc/rbd: refine rbd-exclusive-locks.rst (`pr#49597 <https://github.com/ceph/ceph/pull/49597>`_, Zac Dover)
* doc/rbd: refine rbd-snapshot.rst (`pr#49484 <https://github.com/ceph/ceph/pull/49484>`_, Zac Dover)
* doc/rbd: remove typo and ill-formed command (`pr#49365 <https://github.com/ceph/ceph/pull/49365>`_, Zac Dover)
* doc/rbd: s/wuold/would/ in rados-rbd-cmds.rst (`pr#49591 <https://github.com/ceph/ceph/pull/49591>`_, Zac Dover)
* doc/rbd: update iSCSI gateway info (`pr#49068 <https://github.com/ceph/ceph/pull/49068>`_, Zac Dover)
* doc/releases: improve grammar in pacific.rst (`pr#48424 <https://github.com/ceph/ceph/pull/48424>`_, Zac Dover)
* doc/rgw - fix grammar in table in s3.rst (`pr#50388 <https://github.com/ceph/ceph/pull/50388>`_, Zac Dover)
* doc/rgw: "Migrating Single Site to Multi-Site" (`pr#50093 <https://github.com/ceph/ceph/pull/50093>`_, Zac Dover)
* doc/rgw: caption a diagram (`pr#50293 <https://github.com/ceph/ceph/pull/50293>`_, Zac Dover)
* doc/rgw: clarify multisite.rst top matter (`pr#50204 <https://github.com/ceph/ceph/pull/50204>`_, Zac Dover)
* doc/rgw: clean zone-sync.svg (`pr#50362 <https://github.com/ceph/ceph/pull/50362>`_, Zac Dover)
* doc/rgw: fix caption (`pr#50395 <https://github.com/ceph/ceph/pull/50395>`_, Zac Dover)
* doc/rgw: improve diagram caption (`pr#50331 <https://github.com/ceph/ceph/pull/50331>`_, Zac Dover)
* doc/rgw: multisite ref. top matter cleanup (`pr#50189 <https://github.com/ceph/ceph/pull/50189>`_, Zac Dover)
* doc/rgw: refine "Configuring Secondary Zones" (`pr#50074 <https://github.com/ceph/ceph/pull/50074>`_, Zac Dover)
* doc/rgw: refine "Failover and Disaster Recovery" (`pr#50078 <https://github.com/ceph/ceph/pull/50078>`_, Zac Dover)
* doc/rgw: refine "Multi-site Config Ref" (1 of x) (`pr#50117 <https://github.com/ceph/ceph/pull/50117>`_, Zac Dover)
* doc/rgw: refine "Realms" section (`pr#50139 <https://github.com/ceph/ceph/pull/50139>`_, Zac Dover)
* doc/rgw: refine "Zones" in multisite.rst (`pr#49982 <https://github.com/ceph/ceph/pull/49982>`_, Zac Dover)
* doc/rgw: refine 1-50 of multisite.rst (`pr#49995 <https://github.com/ceph/ceph/pull/49995>`_, Zac Dover)
* doc/rgw: refine keycloak.rst (`pr#50378 <https://github.com/ceph/ceph/pull/50378>`_, Zac Dover)
* doc/rgw: refine multisite to "config 2ndary zones" (`pr#50031 <https://github.com/ceph/ceph/pull/50031>`_, Zac Dover)
* doc/rgw: refine ~50-~140 of multisite.rst (`pr#50008 <https://github.com/ceph/ceph/pull/50008>`_, Zac Dover)
* doc/rgw: remove "tertiary", link to procedure (`pr#50287 <https://github.com/ceph/ceph/pull/50287>`_, Zac Dover)
* doc/rgw: s/[Zz]one [Gg]roup/zonegroup/g (`pr#50136 <https://github.com/ceph/ceph/pull/50136>`_, Zac Dover)
* doc/rgw: session-tags.rst - fix link to keycloak (`pr#50187 <https://github.com/ceph/ceph/pull/50187>`_, Zac Dover)
* doc/security: improve grammar in CVE-2022-0670.rst (`pr#48430 <https://github.com/ceph/ceph/pull/48430>`_, Zac Dover)
* doc/start: add Anthony D'Atri's suggestions (`pr#49615 <https://github.com/ceph/ceph/pull/49615>`_, Zac Dover)
* doc/start: add link-related metadocumentation (`pr#49608 <https://github.com/ceph/ceph/pull/49608>`_, Zac Dover)
* doc/start: add RST escape character rules for bold (`pr#49751 <https://github.com/ceph/ceph/pull/49751>`_, Zac Dover)
* doc/start: improve documenting-ceph.rst (`pr#49565 <https://github.com/ceph/ceph/pull/49565>`_, Zac Dover)
* doc/start: refine "Quirks of RST" (`pr#49610 <https://github.com/ceph/ceph/pull/49610>`_, Zac Dover)
* doc/start: update documenting-ceph.rst (`pr#49570 <https://github.com/ceph/ceph/pull/49570>`_, Zac Dover)
* doc/various: update link to CRUSH pdf (`pr#48402 <https://github.com/ceph/ceph/pull/48402>`_, Zac Dover)
* doc: add releases links to toc (`pr#48945 <https://github.com/ceph/ceph/pull/48945>`_, Patrick Donnelly)
* doc: add the damage types that scrub can repair (`pr#49932 <https://github.com/ceph/ceph/pull/49932>`_, Neeraj Pratap Singh)
* doc: Change 'ReST' to 'REST' in doc/radosgw/layout.rst (`pr#48653 <https://github.com/ceph/ceph/pull/48653>`_, wangyingbin)
* doc: document debugging for libcephsqlite (`pr#50035 <https://github.com/ceph/ceph/pull/50035>`_, Patrick Donnelly)
* doc: document the relevance of mds_namespace mount option (`pr#49689 <https://github.com/ceph/ceph/pull/49689>`_, Jos Collin)
* doc: fix a couple grammatical things (`pr#49621 <https://github.com/ceph/ceph/pull/49621>`_, Brad Fitzpatrick)
* doc: fix a typo (`pr#49683 <https://github.com/ceph/ceph/pull/49683>`_, Brad Fitzpatrick)
* doc: Fix disaster recovery doc (`pr#48343 <https://github.com/ceph/ceph/pull/48343>`_, Kotresh HR)
* doc: Install graphviz (`pr#48904 <https://github.com/ceph/ceph/pull/48904>`_, David Galloway)
* doc: point to main branch for release info (`pr#48800 <https://github.com/ceph/ceph/pull/48800>`_, Patrick Donnelly)
* doc: preen cephadm/troubleshooting.rst and radosgw/placement.rst (`pr#50228 <https://github.com/ceph/ceph/pull/50228>`_, Anthony D'Atri)
* docs: correct add system user to the master zone command (`pr#48655 <https://github.com/ceph/ceph/pull/48655>`_, Salar Nosrati-Ershad)
* drive_group: fix limit filter in drive_selection.selector (`pr#50370 <https://github.com/ceph/ceph/pull/50370>`_, Guillaume Abrioux)
* exporter: avoid stoi for empty pid_str (`pr#48206 <https://github.com/ceph/ceph/pull/48206>`_, Avan Thakkar)
* exporter: don't skip loop if pid path is empty (`pr#48225 <https://github.com/ceph/ceph/pull/48225>`_, Avan Thakkar)
* Fix chown to unlink (`pr#49794 <https://github.com/ceph/ceph/pull/49794>`_, Daniel Gryniewicz)
* fsmap: switch to using iterator based loop (`pr#48268 <https://github.com/ceph/ceph/pull/48268>`_, Aliaksei Makarau)
* librbd/cache/pwl: fix clean vs bytes_dirty cache state inconsistency (`pr#49055 <https://github.com/ceph/ceph/pull/49055>`_, Yin Congmin)
* librbd: avoid EUCLEAN error after "rbd rm" is interrupted (`pr#50130 <https://github.com/ceph/ceph/pull/50130>`_, weixinwei)
* librbd: call apply_changes() after setting librados_thread_count (`pr#50292 <https://github.com/ceph/ceph/pull/50292>`_, Ilya Dryomov)
* librbd: compare-and-write fixes and vector C API (`pr#48474 <https://github.com/ceph/ceph/pull/48474>`_, Ilya Dryomov, Jonas Pfefferle)
* librbd: Fix local rbd mirror journals growing forever (`pr#50159 <https://github.com/ceph/ceph/pull/50159>`_, Ilya Dryomov, Josef Johansson)
* make-dist: don't set Release tag in ceph.spec for SUSE distros (`pr#48613 <https://github.com/ceph/ceph/pull/48613>`_, Tim Serong, Nathan Cutler)
* mds/client: fail the request if the peer MDS doesn't support getvxattr op (`pr#47890 <https://github.com/ceph/ceph/pull/47890>`_, Zack Cerza, Xiubo Li)
* mds/PurgeQueue: don't consider filer_max_purge_ops when _calculate_ops (`pr#49655 <https://github.com/ceph/ceph/pull/49655>`_, haoyixing)
* mds/Server: Do not abort MDS on unknown messages (`pr#48252 <https://github.com/ceph/ceph/pull/48252>`_, Dhairya Parmar, Dhairy Parmar)
* mds: account for snapshot items when deciding to split or merge a directory (`issue#55215 <http://tracker.ceph.com/issues/55215>`_, `pr#49673 <https://github.com/ceph/ceph/pull/49673>`_, Venky Shankar)
* mds: avoid ~mdsdir's scrubbing and reporting damage health status (`pr#49473 <https://github.com/ceph/ceph/pull/49473>`_, Neeraj Pratap Singh)
* mds: damage table only stores one dentry per dirfrag (`pr#48261 <https://github.com/ceph/ceph/pull/48261>`_, Patrick Donnelly)
* mds: do not acquire xlock in xlockdone state (`pr#49539 <https://github.com/ceph/ceph/pull/49539>`_, Igor Fedotov)
* mds: fix and skip submitting invalid osd request (`pr#49939 <https://github.com/ceph/ceph/pull/49939>`_, Xiubo Li)
* mds: fix scan_stray_dir not reset next.frag on each run of stray inode (`pr#49670 <https://github.com/ceph/ceph/pull/49670>`_, ethanwu)
* mds: md_log_replay thread blocks waiting to be woken up (`pr#49672 <https://github.com/ceph/ceph/pull/49672>`_, zhikuodu)
* mds: switch submit_mutex to fair mutex for MDLog (`pr#49633 <https://github.com/ceph/ceph/pull/49633>`_, Xiubo Li)
* mds: wait unlink to finish to avoid conflict when creating same entries (`pr#48452 <https://github.com/ceph/ceph/pull/48452>`_, Xiubo Li)
* mgr/cephadm: add ingress support for ssl rgw service (`pr#49865 <https://github.com/ceph/ceph/pull/49865>`_, Frank Ederveen)
* mgr/cephadm: allow setting prometheus retention time (`pr#47943 <https://github.com/ceph/ceph/pull/47943>`_, Redouane Kachach, Adam King)
* mgr/cephadm: call iscsi post_remove from serve loop (`pr#49847 <https://github.com/ceph/ceph/pull/49847>`_, Adam King)
* mgr/cephadm: don't say migration in progress if migration current > migration last (`pr#49861 <https://github.com/ceph/ceph/pull/49861>`_, Adam King)
* mgr/cephadm: don't use "sudo" in commands if user is root (`pr#48079 <https://github.com/ceph/ceph/pull/48079>`_, Adam King)
* mgr/cephadm: fix backends service in haproxy config with multiple nfs of same rank (`pr#50446 <https://github.com/ceph/ceph/pull/50446>`_, Adam King)
* mgr/cephadm: fix check for if devices have changed (`pr#49864 <https://github.com/ceph/ceph/pull/49864>`_, Adam King)
* mgr/cephadm: fix handling of mgr upgrades with 3 or more mgrs (`pr#49859 <https://github.com/ceph/ceph/pull/49859>`_, Adam King)
* mgr/cephadm: fix removing offline hosts with ingress daemons (`pr#49850 <https://github.com/ceph/ceph/pull/49850>`_, Adam King)
* mgr/cephadm: fix tuned profiles getting removed if name has dashes (`pr#48077 <https://github.com/ceph/ceph/pull/48077>`_, Adam King)
* mgr/cephadm: improve offline host handling, mostly around upgrade (`pr#49856 <https://github.com/ceph/ceph/pull/49856>`_, Adam King)
* mgr/cephadm: increase ingress timeout values (`pr#49853 <https://github.com/ceph/ceph/pull/49853>`_, Frank Ederveen)
* mgr/cephadm: iscsi username and password defaults to admin (`pr#49309 <https://github.com/ceph/ceph/pull/49309>`_, Nizamudeen A)
* mgr/cephadm: make logging refresh metadata to debug logs configurable (`pr#49857 <https://github.com/ceph/ceph/pull/49857>`_, Adam King)
* mgr/cephadm: make setting --cgroups=split configurable (`pr#48075 <https://github.com/ceph/ceph/pull/48075>`_, Adam King)
* mgr/cephadm: reconfig iscsi daemons if trusted_ip_list changes (`pr#48076 <https://github.com/ceph/ceph/pull/48076>`_, Adam King)
* mgr/cephadm: save host cache data after scheduling daemon action (`pr#49863 <https://github.com/ceph/ceph/pull/49863>`_, Adam King)
* mgr/cephadm: some master -> main cleanup (`pr#49284 <https://github.com/ceph/ceph/pull/49284>`_, Adam King)
* mgr/cephadm: specify ports for iscsi (`pr#49862 <https://github.com/ceph/ceph/pull/49862>`_, Adam King)
* mgr/cephadm: support for extra entrypoint args (`pr#49851 <https://github.com/ceph/ceph/pull/49851>`_, Adam King)
* mgr/cephadm: try to avoid pull when getting container image info (`pr#50170 <https://github.com/ceph/ceph/pull/50170>`_, Mykola Golub, Adam King)
* mgr/cephadm: validating tuned profile specification (`pr#48078 <https://github.com/ceph/ceph/pull/48078>`_, Redouane Kachach)
* mgr/cephadm: write client files after applying services (`pr#49860 <https://github.com/ceph/ceph/pull/49860>`_, Adam King)
* mgr/dashboard: Add a Silence button shortcut to alert notifications (`pr#48065 <https://github.com/ceph/ceph/pull/48065>`_, Nizamudeen A, Aashish Sharma)
* mgr/dashboard: Add details to the modal which displays the `safe-to-d… (`pr#48177 <https://github.com/ceph/ceph/pull/48177>`_, Francesco Torchia)
* mgr/dashboard: Add metric relative to osd blocklist (`pr#49501 <https://github.com/ceph/ceph/pull/49501>`_, Aashish Sharma)
* mgr/dashboard: add option to resolve ip addr (`pr#48219 <https://github.com/ceph/ceph/pull/48219>`_, Tatjana Dehler)
* mgr/dashboard: add server side encryption to rgw/s3 (`pr#48441 <https://github.com/ceph/ceph/pull/48441>`_, Aashish Sharma)
* mgr/dashboard: Add text to empty life expectancy column (`pr#48271 <https://github.com/ceph/ceph/pull/48271>`_, Francesco Torchia)
* mgr/dashboard: add tooltip mirroring pools table (`pr#49504 <https://github.com/ceph/ceph/pull/49504>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: allow cross origin when the url is set (`pr#49150 <https://github.com/ceph/ceph/pull/49150>`_, Avan Thakkar, Nizamudeen A)
* mgr/dashboard: backport of all accessibility changes (`pr#49727 <https://github.com/ceph/ceph/pull/49727>`_, nsedrickm)
* mgr/dashboard: bug fixes for rbd mirroring edit and promotion/demotion (`pr#48807 <https://github.com/ceph/ceph/pull/48807>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: cephadm dashboard e2e fixes (`pr#50450 <https://github.com/ceph/ceph/pull/50450>`_, Nizamudeen A)
* mgr/dashboard: custom image for kcli bootstrap script (`pr#50459 <https://github.com/ceph/ceph/pull/50459>`_, Nizamudeen A)
* mgr/dashboard: display real health in rbd mirroring pools (`pr#49518 <https://github.com/ceph/ceph/pull/49518>`_, Pere Diaz Bou)
* mgr/dashboard: fix "can't read .ssh/known_hosts: No such file or directory (`pr#47957 <https://github.com/ceph/ceph/pull/47957>`_, Nizamudeen A)
* mgr/dashboard: Fix broken Fedora image URL (`pr#48340 <https://github.com/ceph/ceph/pull/48340>`_, Zack Cerza, Nizamudeen A)
* mgr/dashboard: fix bucket encryption checkbox (`pr#49776 <https://github.com/ceph/ceph/pull/49776>`_, Aashish Sharma)
* mgr/dashboard: fix CephPGImbalance alert (`pr#49476 <https://github.com/ceph/ceph/pull/49476>`_, Aashish Sharma)
* mgr/dashboard: Fix CephPoolGrowthWarning alert (`pr#49475 <https://github.com/ceph/ceph/pull/49475>`_, Aashish Sharma)
* mgr/dashboard: fix constraints.txt (`pr#50234 <https://github.com/ceph/ceph/pull/50234>`_, Ernesto Puerta)
* mgr/dashboard: fix Expected to find element: `cd-modal .badge but never found it (`pr#48141 <https://github.com/ceph/ceph/pull/48141>`_, Nizamudeen A)
* mgr/dashboard: fix openapi-check (`pr#48046 <https://github.com/ceph/ceph/pull/48046>`_, Pere Diaz Bou)
* mgr/dashboard: fix rbd mirroring daemon health status (`pr#50125 <https://github.com/ceph/ceph/pull/50125>`_, Nizamudeen A)
* mgr/dashboard: fix rgw connect when using ssl (`issue#56970 <http://tracker.ceph.com/issues/56970>`_, `pr#48188 <https://github.com/ceph/ceph/pull/48188>`_, Henry Hirsch)
* mgr/dashboard: fix server side encryption config error (`pr#49481 <https://github.com/ceph/ceph/pull/49481>`_, Aashish Sharma)
* mgr/dashboard: fix snapshot creation with duplicate name (`pr#48047 <https://github.com/ceph/ceph/pull/48047>`_, Aashish Sharma)
* mgr/dashboard: fix weird data in osd details (`pr#48433 <https://github.com/ceph/ceph/pull/48433>`_, Pedro Gonzalez Gomez, Nizamudeen A)
* mgr/dashboard: handle the cephfs permission issue in nfs exports (`pr#48315 <https://github.com/ceph/ceph/pull/48315>`_, Nizamudeen A)
* mgr/dashboard: move service_instances logic to backend (`pr#50451 <https://github.com/ceph/ceph/pull/50451>`_, Nizamudeen A)
* mgr/dashboard: osd form preselect db/wal device filters (`pr#48115 <https://github.com/ceph/ceph/pull/48115>`_, Nizamudeen A)
* mgr/dashboard: paginate services (`pr#48788 <https://github.com/ceph/ceph/pull/48788>`_, Melissa Li, Pere Diaz Bou)
* mgr/dashboard: rbd-mirror improvements (`pr#49499 <https://github.com/ceph/ceph/pull/49499>`_, Aashish Sharma)
* mgr/dashboard: refactor dashboard cephadm e2e tests (`pr#48432 <https://github.com/ceph/ceph/pull/48432>`_, Nizamudeen A)
* mgr/dashboard: Replace vonage-status-panel with native grafana stat panel (`pr#50043 <https://github.com/ceph/ceph/pull/50043>`_, Aashish Sharma)
* mgr/dashboard: rgw server side encryption config values set to wrong daemon (`pr#49724 <https://github.com/ceph/ceph/pull/49724>`_, Aashish Sharma)
* mgr/dashboard: Unable to change rgw subuser permission (`pr#48440 <https://github.com/ceph/ceph/pull/48440>`_, Aashish Sharma)
* mgr/dashboard: upgrade to angular 13, bootstrap 5 and jest 28 (`pr#50124 <https://github.com/ceph/ceph/pull/50124>`_, Nizamudeen A, Bryan Montalvan)
* mgr/nfs: add sectype option (`pr#48531 <https://github.com/ceph/ceph/pull/48531>`_, John Mulligan)
* mgr/nfs: handle bad cluster name during info command (`pr#49654 <https://github.com/ceph/ceph/pull/49654>`_, Dhairya Parmar)
* mgr/orchestrator: fix upgrade status help message (`pr#49855 <https://github.com/ceph/ceph/pull/49855>`_, Adam King)
* mgr/prometheus: change pg_repaired_objects name to pool_repaired_objects (`pr#48438 <https://github.com/ceph/ceph/pull/48438>`_, Pere Diaz Bou)
* mgr/prometheus: export zero valued pg state metrics (`pr#49787 <https://github.com/ceph/ceph/pull/49787>`_, Avan Thakkar)
* mgr/prometheus: expose daemon health metrics (`pr#49519 <https://github.com/ceph/ceph/pull/49519>`_, Pere Diaz Bou)
* mgr/prometheus: expose repaired pgs metrics (`pr#48204 <https://github.com/ceph/ceph/pull/48204>`_, Pere Diaz Bou)
* mgr/prometheus: fix module crash when trying to collect OSDs metrics (`pr#49930 <https://github.com/ceph/ceph/pull/49930>`_, Redouane Kachach)
* mgr/prometheus: use vendored "packaging" instead (`pr#49698 <https://github.com/ceph/ceph/pull/49698>`_, Kefu Chai, Matan Breizman)
* mgr/rbd_support: avoid wedging the task queue if pool is removed (`pr#49057 <https://github.com/ceph/ceph/pull/49057>`_, Ilya Dryomov)
* mgr/rbd_support: remove localized schedule option during module startup (`pr#49649 <https://github.com/ceph/ceph/pull/49649>`_, Ramana Raja)
* mgr/rook: Device inventory (`pr#49877 <https://github.com/ceph/ceph/pull/49877>`_, Juan Miguel Olmo Martínez)
* mgr/rook:NFSRados constructor expects type of rados as a parameter instead of MgrModule (`pr#48830 <https://github.com/ceph/ceph/pull/48830>`_, Ben Gao)
* mgr/snap_schedule: remove subvol interface (`pr#48222 <https://github.com/ceph/ceph/pull/48222>`_, Milind Changire)
* mgr/telemetry: add `basic_pool_options_bluestore` collection (`pr#49414 <https://github.com/ceph/ceph/pull/49414>`_, Laura Flores)
* mgr/telemetry: handle daemons with complex ids (`pr#48283 <https://github.com/ceph/ceph/pull/48283>`_, Laura Flores)
* mgr/volumes: Add human-readable flag to volume info command (`pr#48466 <https://github.com/ceph/ceph/pull/48466>`_, Neeraj Pratap Singh)
* mgr: Fix prettytable pinning to restore python3.6 (`pr#48297 <https://github.com/ceph/ceph/pull/48297>`_, Zack Cerza)
* mon, osd: rework the public_bind_addr support. Bring it to OSD (`pr#50153 <https://github.com/ceph/ceph/pull/50153>`_, Radosław Zarzyński, Radoslaw Zarzynski)
* mon,auth,cephadm: support auth key rotation (`pr#48093 <https://github.com/ceph/ceph/pull/48093>`_, Adam King, Radoslaw Zarzynski, Sage Weil)
* mon/Elector.cc: Compress peer >= rank_size sanity check into send_peer_ping (`pr#49433 <https://github.com/ceph/ceph/pull/49433>`_, Kamoltat)
* mon/Elector: Added sanity check when pinging a peer monitor (`pr#48321 <https://github.com/ceph/ceph/pull/48321>`_, Kamoltat)
* mon/Elector: Change how we handle removed_ranks and notify_rank_removed() (`pr#49311 <https://github.com/ceph/ceph/pull/49311>`_, Kamoltat)
* mon/LogMonitor: Fix log last (`pr#50407 <https://github.com/ceph/ceph/pull/50407>`_, Prashant D)
* mon/MgrMap: dump last_failure_osd_epoch and active_clients at top level (`pr#50306 <https://github.com/ceph/ceph/pull/50306>`_, Ilya Dryomov)
* mon/MonCommands: Support dump_historic_slow_ops (`pr#49232 <https://github.com/ceph/ceph/pull/49232>`_, Matan Breizman)
* mon/OSDMointor: Simplify check_pg_num() (`pr#50327 <https://github.com/ceph/ceph/pull/50327>`_, Matan Breizman, Anthony D'Atri, Tongliang Deng, Jerry Luo)
* mon: bail from handle_command() if _generate_command_map() fails (`pr#48845 <https://github.com/ceph/ceph/pull/48845>`_, Nikhil Kshirsagar)
* mon: disable snap id allocation for fsmap pools (`pr#50090 <https://github.com/ceph/ceph/pull/50090>`_, Milind Changire)
* mon: Fix condition to check for ceph version mismatch (`pr#49989 <https://github.com/ceph/ceph/pull/49989>`_, Prashant D)
* Monitor: forward report command to leader (`pr#47928 <https://github.com/ceph/ceph/pull/47928>`_, Dan van der Ster)
* monitoring/ceph-mixin: add RGW host to label info (`pr#48034 <https://github.com/ceph/ceph/pull/48034>`_, Tatjana Dehler)
* mount: fix mount failure with old kernels (`pr#49404 <https://github.com/ceph/ceph/pull/49404>`_, Xiubo Li)
* os/bluesore: cumulative backport for Onode stuff and more (`pr#50048 <https://github.com/ceph/ceph/pull/50048>`_, Igor Fedotov, Adam Kupczyk)
* os/bluestore: BlueFS: harmonize log read and writes modes (`pr#50474 <https://github.com/ceph/ceph/pull/50474>`_, Adam Kupczyk)
* os/bluestore: enable 4K allocation unit for BlueFS (`pr#49884 <https://github.com/ceph/ceph/pull/49884>`_, Igor Fedotov)
* os/memstore: Fix memory leak (`pr#50091 <https://github.com/ceph/ceph/pull/50091>`_, Adam Kupczyk)
* osd: add created_at meta (`pr#49159 <https://github.com/ceph/ceph/pull/49159>`_, Alex Marangone)
* osd: add scrub duration for scrubs after recovery (`pr#47926 <https://github.com/ceph/ceph/pull/47926>`_, Aishwarya Mathuria)
* osd: Implement Context based completion for mon cmd to set a config option (`pr#47983 <https://github.com/ceph/ceph/pull/47983>`_, Sridhar Seshasayee)
* osd: mds: suggest clock skew when failing to obtain rotating service keys (`pr#50405 <https://github.com/ceph/ceph/pull/50405>`_, Greg Farnum)
* osd: Randomize osd bench buffer data before submitting to objectstore (`pr#49323 <https://github.com/ceph/ceph/pull/49323>`_, Sridhar Seshasayee)
* osd: Reduce backfill/recovery default limits for mClock and other optimizations (`pr#49437 <https://github.com/ceph/ceph/pull/49437>`_, Sridhar Seshasayee)
* osd: remove invalid put on message (`pr#48039 <https://github.com/ceph/ceph/pull/48039>`_, Nitzan Mordechai)
* osd: Reset mClock's OSD capacity config option for inactive device type (`pr#49281 <https://github.com/ceph/ceph/pull/49281>`_, Sridhar Seshasayee)
* osd: Restore defaults of mClock built-in profiles upon modification (`pr#50097 <https://github.com/ceph/ceph/pull/50097>`_, Sridhar Seshasayee)
* osd: shut down the MgrClient before osd_fast_shutdown (`pr#49881 <https://github.com/ceph/ceph/pull/49881>`_, Laura Flores, Brad Hubbard)
* osd/scrub: use the actual active set when requesting replicas… (`pr#48543 <https://github.com/ceph/ceph/pull/48543>`_, Ronen Friedman)
* PendingReleaseNotes: document online and offline trimming of PG Log's… (`pr#48019 <https://github.com/ceph/ceph/pull/48019>`_, Radoslaw Zarzynski)
* pybind/mgr/autoscaler: Do not show NEW PG_NUM value if autoscaler is not on (`pr#47925 <https://github.com/ceph/ceph/pull/47925>`_, Prashant D)
* pybind/mgr: check for empty metadata mgr_module:get_metadata() (`issue#57072 <http://tracker.ceph.com/issues/57072>`_, `pr#49967 <https://github.com/ceph/ceph/pull/49967>`_, Venky Shankar)
* pybind/mgr: fix tox autopep8 args flake8 (`pr#49505 <https://github.com/ceph/ceph/pull/49505>`_, Aashish Sharma)
* pybind/mgr: fixup after upgrading tox versions (`pr#49361 <https://github.com/ceph/ceph/pull/49361>`_, Kefu Chai, Adam King)
* pybind/mgr: object_format.py decorator updates & docs (`pr#47979 <https://github.com/ceph/ceph/pull/47979>`_, John Mulligan)
* pybind/mgr: tox and test fixes (`pr#49508 <https://github.com/ceph/ceph/pull/49508>`_, Kefu Chai)
* pybind/mgr: use memory temp_store for sqlite3 db (`pr#50286 <https://github.com/ceph/ceph/pull/50286>`_, Patrick Donnelly)
* pybind/rados: notify callback reconnect (`pr#48113 <https://github.com/ceph/ceph/pull/48113>`_, Nitzan Mordechai)
* python-common: Add 'KB' to supported suffixes in SizeMatcher (`pr#48242 <https://github.com/ceph/ceph/pull/48242>`_, Tim Serong)
* qa/cephadm: remove fsid dir before bootstrap in test_cephadm.sh (`pr#47949 <https://github.com/ceph/ceph/pull/47949>`_, Adam King)
* qa/suites/rbd: fix sporadic "rx-only direction" test failures (`pr#50113 <https://github.com/ceph/ceph/pull/50113>`_, Ilya Dryomov)
* qa/suites/rgw: fix and update tempest and barbican tests (`pr#50002 <https://github.com/ceph/ceph/pull/50002>`_, Tobias Urdin)
* qa/tasks/cephadm.py: fix pulling cephadm from git.ceph.com (`pr#49858 <https://github.com/ceph/ceph/pull/49858>`_, Adam King)
* qa/tasks/kubeadm: set up tigera resources via kubectl create (`pr#48080 <https://github.com/ceph/ceph/pull/48080>`_, John Mulligan)
* qa/tasks/rbd_fio: bump default to fio 3.32 (`pr#48386 <https://github.com/ceph/ceph/pull/48386>`_, Ilya Dryomov)
* qa/tests: added quincy client upgrade => reef (`pr#50353 <https://github.com/ceph/ceph/pull/50353>`_, Yuri Weinstein)
* qa/tests: initial draft for quincy p2p tests (`pr#46896 <https://github.com/ceph/ceph/pull/46896>`_, Yuri Weinstein, Laura Flores)
* qa/workunits/rados: specify redirect in curl command (`pr#49140 <https://github.com/ceph/ceph/pull/49140>`_, Laura Flores)
* qa/workunits/windows: backport rbd-wnbd tests (`pr#49883 <https://github.com/ceph/ceph/pull/49883>`_, Lucian Petrut)
* qa: Fix test_subvolume_group_ls_filter_internal_directories (`pr#48327 <https://github.com/ceph/ceph/pull/48327>`_, Kotresh HR)
* qa: Fix test_subvolume_snapshot_info_if_orphan_clone (`pr#48325 <https://github.com/ceph/ceph/pull/48325>`_, Kotresh HR)
* qa: ignore disk quota exceeded failure in test (`pr#48164 <https://github.com/ceph/ceph/pull/48164>`_, Nikhilkumar Shelke)
* qa: switch back to git protocol for qemu-xfstests (`pr#49544 <https://github.com/ceph/ceph/pull/49544>`_, Ilya Dryomov)
* qa: switch to https protocol for repos' server (`pr#49471 <https://github.com/ceph/ceph/pull/49471>`_, Xiubo Li)
* qa: wait for scrub to finish (`pr#49459 <https://github.com/ceph/ceph/pull/49459>`_, Milind Changire)
* rbd-mirror: add information about the last snapshot sync to image status (`pr#50266 <https://github.com/ceph/ceph/pull/50266>`_, Divyansh Kamboj)
* rbd-mirror: fix syncing_percent calculation logic in get_replay_status() (`pr#50180 <https://github.com/ceph/ceph/pull/50180>`_, N Balachandran)
* rbd: add --snap-id option to "rbd device map" to allow mapping arbitrary snapshots (`pr#49197 <https://github.com/ceph/ceph/pull/49197>`_, Ilya Dryomov, Prasanna Kumar Kalever)
* rbd: device map/unmap --namespace handling fixes (`pr#48458 <https://github.com/ceph/ceph/pull/48458>`_, Ilya Dryomov, Stefan Chivu)
* RGW - Make sure PostObj set bucket on s->object (`pr#49641 <https://github.com/ceph/ceph/pull/49641>`_, Daniel Gryniewicz)
* rgw multisite: replicate metadata for iam roles (`pr#48030 <https://github.com/ceph/ceph/pull/48030>`_, Pritha Srivastava, Abhishek Lekshmanan)
* rgw/beast: fix interaction between keepalive and 100-continue (`pr#49840 <https://github.com/ceph/ceph/pull/49840>`_, Casey Bodley)
* rgw/beast: StreamIO remembers connection errors for graceful shutdown (`pr#50239 <https://github.com/ceph/ceph/pull/50239>`_, Casey Bodley)
* rgw/coroutine: check for null stack on wakeup (`pr#49096 <https://github.com/ceph/ceph/pull/49096>`_, Casey Bodley)
* rgw: "reshard cancel" errors with "invalid argument" (`pr#49090 <https://github.com/ceph/ceph/pull/49090>`_, J. Eric Ivancich)
* rgw: add 'inline_data' zone placement info option (`pr#50209 <https://github.com/ceph/ceph/pull/50209>`_, Cory Snyder)
* rgw: adding BUCKET_REWRITE and OBJECT_REWRITE OPS to (`pr#49094 <https://github.com/ceph/ceph/pull/49094>`_, Pritha Srivastava)
* rgw: address bug where object puts could write to decommissioned shard (`pr#49795 <https://github.com/ceph/ceph/pull/49795>`_, J. Eric Ivancich)
* rgw: Backport of issue 57562 to Quincy (`pr#49679 <https://github.com/ceph/ceph/pull/49679>`_, Adam C. Emerson)
* rgw: bucket list operation slow down in special scenario (`pr#49085 <https://github.com/ceph/ceph/pull/49085>`_, zealot)
* rgw: default-initialize delete_multi_obj_op_meta (`pr#50184 <https://github.com/ceph/ceph/pull/50184>`_, Casey Bodley)
* rgw: fix bool/int logic error when calling get_obj_head_ioctx (`pr#48231 <https://github.com/ceph/ceph/pull/48231>`_, J. Eric Ivancich)
* rgw: fix bug where variable referenced after data moved out (`pr#48228 <https://github.com/ceph/ceph/pull/48228>`_, J. Eric Ivancich)
* rgw: fix data corruption due to network jitter (`pr#48273 <https://github.com/ceph/ceph/pull/48273>`_, Shasha Lu)
* rgw: Fix segfault due to concurrent socket use at timeout (`pr#50240 <https://github.com/ceph/ceph/pull/50240>`_, Yixin Jin)
* rgw: fix segfault in UserAsyncRefreshHandler::init_fetch (`pr#49083 <https://github.com/ceph/ceph/pull/49083>`_, Cory Snyder)
* rgw: fix the problem of duplicate idx when bi list (`pr#49828 <https://github.com/ceph/ceph/pull/49828>`_, wangtengfei)
* rgw: Fix truncated ListBuckets response (`pr#49525 <https://github.com/ceph/ceph/pull/49525>`_, Joshua Baergen)
* rgw: log deletion status of individual objects in multi object delete request (`pr#49084 <https://github.com/ceph/ceph/pull/49084>`_, Cory Snyder)
* rgw: prevent spurious/lost notifications in the index completion thread (`pr#49092 <https://github.com/ceph/ceph/pull/49092>`_, Casey Bodley, Yuval Lifshitz)
* rgw: remove guard_reshard in bucket_index_read_olh_log (`pr#49775 <https://github.com/ceph/ceph/pull/49775>`_, Mingyuan Liang)
* rgw: RGWPutLC does not require Content-MD5 (`pr#49088 <https://github.com/ceph/ceph/pull/49088>`_, Casey Bodley)
* rgw: splitting gc chains into smaller parts to prevent (`pr#48239 <https://github.com/ceph/ceph/pull/48239>`_, Pritha Srivastava)
* rgw: x-amz-date change breaks certain cases of aws sig v4 (`pr#48312 <https://github.com/ceph/ceph/pull/48312>`_, Marcus Watts)
* src/crush: extra logging to debug CPU burn in test_with_fork() (`pr#50406 <https://github.com/ceph/ceph/pull/50406>`_, Deepika Upadhyay)
* src/mds: increment directory inode's change attr by one (`pr#48520 <https://github.com/ceph/ceph/pull/48520>`_, Ramana Raja)
* src/pybind/cephfs: fix grammar (`pr#48981 <https://github.com/ceph/ceph/pull/48981>`_, Zac Dover)
* src/pybind: fix typo in cephfs.pyx (`pr#48952 <https://github.com/ceph/ceph/pull/48952>`_, Zac Dover)
* src/valgrind.supp: Adding know leaks unrelated to ceph (`pr#49522 <https://github.com/ceph/ceph/pull/49522>`_, Nitzan Mordechai)
* tests: remove pubsub tests from multisite (`pr#48914 <https://github.com/ceph/ceph/pull/48914>`_, Yuval Lifshitz)
* v17.2.5 (`pr#48519 <https://github.com/ceph/ceph/pull/48519>`_, Ceph Release Team, Laura Flores, Guillaume Abrioux, Juan Miguel Olmo Martínez)
* Wip doc 2022 11 21 backport 48975 to quincy (`pr#48976 <https://github.com/ceph/ceph/pull/48976>`_, Zac Dover)


v17.2.5 Quincy
==============

This is a hotfix release that addresses missing commits in the 17.2.4 release.
We recommend that all users update to this release.

Related tracker: https://tracker.ceph.com/issues/57858

Notable Changes
---------------

* A ceph-volume regression introduced in bea9f4b that makes the
  activate process take a very long time to complete has been
  fixed.

  Related tracker: https://tracker.ceph.com/issues/57627

* An exception that occurs with some NFS commands
  in Rook clusters has been fixed.

  Related tracker: https://tracker.ceph.com/issues/55605

* A crash in the Telemetry module that may affect some users opted
  into the perf channel has been fixed.

  Related tracker: https://tracker.ceph.com/issues/57700

Changelog
---------

* ceph-volume: fix regression in activate (`pr#48201 <https://github.com/ceph/ceph/pull/48201>`_, Guillaume Abrioux)
* mgr/rook: fix error when trying to get the list of nfs services (`pr#48199 <https://github.com/ceph/ceph/pull/48199>`_, Juan Miguel Olmo)
* mgr/telemetry: handle daemons with complex ids (`pr#48283 <https://github.com/ceph/ceph/pull/48283>`_, Laura Flores)
* Revert PR 47901 (`pr#48104 <https://github.com/ceph/ceph/pull/48104>`_, Laura Flores)

v17.2.4 Quincy
==============

This is the fourth backport release in the Quincy series. We recommend
that all users update to this release.

Notable Changes
---------------

* Cephfs: The ``AT_NO_ATTR_SYNC`` macro is deprecated, please use the standard
  ``AT_STATX_DONT_SYNC`` macro. The ``AT_NO_ATTR_SYNC`` macro will be removed in
  the future.

* OSD: The issue of high CPU utilization during recovery/backfill operations
  has been fixed. For more details see: https://tracker.ceph.com/issues/56530.

* Trimming of PGLog dups is now controlled by size instead of the version.
  This fixes the PGLog inflation issue that was happening when online
  (in OSD) trimming jammed after a PG split operation. Also, a new offline
  mechanism has been added: ``ceph-objectstore-tool`` now has a ``trim-pg-log-dups`` op
  that targets situations where an OSD is unable to boot due to those inflated dups.
  If that is the case, in OSD logs the "You can be hit by THE DUPS BUG" warning
  will be visible.
  Relevant tracker: https://tracker.ceph.com/issues/53729

* OSD: Octopus modified the SnapMapper key format from
  ``<LEGACY_MAPPING_PREFIX><snapid>_<shardid>_<hobject_t::to_str()>``
  to
  ``<MAPPING_PREFIX><pool>_<snapid>_<shardid>_<hobject_t::to_str()>``.
  When this change was introduced, `94ebe0e <https://github.com/ceph/ceph/commit/94ebe0eab968068c29fdffa1bfe68c72122db633>`_
  also introduced a conversion with a crucial bug which essentially
  destroyed legacy keys by mapping them to
  ``<MAPPING_PREFIX><poolid>_<snapid>_``
  without the object-unique suffix. The conversion is fixed in this release.
  Relevant tracker: https://tracker.ceph.com/issues/56147

Changelog
---------

* .readthedocs.yml: Always build latest doc/releases pages (`pr#47442 <https://github.com/ceph/ceph/pull/47442>`_, David Galloway)
* Add mapping for ernno:13 and adding path in error msg in opendir()/cephfs.pyx (`pr#46647 <https://github.com/ceph/ceph/pull/46647>`_, Sarthak0702)
* admin: Fix check if PR or release branch docs build (`pr#47739 <https://github.com/ceph/ceph/pull/47739>`_, David Galloway)
* bdev: fix FTBFS on FreeBSD, keep the huge paged read buffers (`pr#44641 <https://github.com/ceph/ceph/pull/44641>`_, Radoslaw Zarzynski)
* build: Silence deprecation warnings from OpenSSL 3 (`pr#47585 <https://github.com/ceph/ceph/pull/47585>`_, Kefu Chai, Adam C. Emerson)
* Catch exception if thrown by __generate_command_map() (`pr#45892 <https://github.com/ceph/ceph/pull/45892>`_, Nikhil Kshirsagar)
* ceph-fuse: add dedicated snap stag map for each directory (`pr#46948 <https://github.com/ceph/ceph/pull/46948>`_, Xiubo Li)
* ceph-mixin: backport of recent cleanups (`pr#46548 <https://github.com/ceph/ceph/pull/46548>`_, Arthur Outhenin-Chalandre)
* ceph-volume: avoid unnecessary subprocess calls (`pr#46968 <https://github.com/ceph/ceph/pull/46968>`_, Guillaume Abrioux)
* ceph-volume: decrease number of `pvs` calls in `lvm list` (`pr#46966 <https://github.com/ceph/ceph/pull/46966>`_, Guillaume Abrioux)
* ceph-volume: do not call get_device_vgs() per devices (`pr#47348 <https://github.com/ceph/ceph/pull/47348>`_, Guillaume Abrioux)
* ceph-volume: do not log sensitive details (`pr#46728 <https://github.com/ceph/ceph/pull/46728>`_, Guillaume Abrioux)
* ceph-volume: fix `simple scan` (`pr#47149 <https://github.com/ceph/ceph/pull/47149>`_, Guillaume Abrioux)
* ceph-volume: fix fast device alloc size on mulitple device (`pr#47293 <https://github.com/ceph/ceph/pull/47293>`_, Arthur Outhenin-Chalandre)
* ceph-volume: fix regression in activate (`pr#48201 <https://github.com/ceph/ceph/pull/48201>`_, Guillaume Abrioux)
* ceph-volume: make is_valid() optional (`pr#46730 <https://github.com/ceph/ceph/pull/46730>`_, Guillaume Abrioux)
* ceph-volume: only warn when config file isn't found (`pr#46070 <https://github.com/ceph/ceph/pull/46070>`_, Guillaume Abrioux)
* ceph-volume: Quincy backports (`pr#47406 <https://github.com/ceph/ceph/pull/47406>`_, Guillaume Abrioux, Zack Cerza, Michael Fritch)
* ceph-volume: system.get_mounts() refactor (`pr#47536 <https://github.com/ceph/ceph/pull/47536>`_, Guillaume Abrioux)
* ceph-volume/tests: fix test_exception_returns_default (`pr#47435 <https://github.com/ceph/ceph/pull/47435>`_, Guillaume Abrioux)
* ceph.spec.in backports (`pr#47549 <https://github.com/ceph/ceph/pull/47549>`_, David Galloway, Kefu Chai, Tim Serong, Casey Bodley, Radoslaw Zarzynski, Radosław Zarzyński)
* ceph.spec.in: disable system_pmdk on s390x (`pr#47251 <https://github.com/ceph/ceph/pull/47251>`_, Ken Dreyer)
* ceph.spec.in: openSUSE: require gcc11-c++, disable parquet (`pr#46155 <https://github.com/ceph/ceph/pull/46155>`_, Tim Serong)
* ceph.spec: fixing cephadm build deps (`pr#47069 <https://github.com/ceph/ceph/pull/47069>`_, Redouane Kachach)
* cephadm/ceph-volume: fix rm-cluster --zap (`pr#47626 <https://github.com/ceph/ceph/pull/47626>`_, Guillaume Abrioux)
* cephadm/mgr: adding logic to handle --no-overwrite for tuned profiles (`pr#47944 <https://github.com/ceph/ceph/pull/47944>`_, Redouane Kachach)
* cephadm: add "su root root" to cephadm.log logrotate config (`pr#47314 <https://github.com/ceph/ceph/pull/47314>`_, Adam King)
* cephadm: add 'is_paused' field in orch status output (`pr#46569 <https://github.com/ceph/ceph/pull/46569>`_, Guillaume Abrioux)
* Cephadm: Allow multiple virtual IP addresses for keepalived and haproxy (`pr#47610 <https://github.com/ceph/ceph/pull/47610>`_, Luis Domingues)
* cephadm: change default keepalived/haproxy container images (`pr#46714 <https://github.com/ceph/ceph/pull/46714>`_, Guillaume Abrioux)
* cephadm: fix incorrect warning (`pr#47608 <https://github.com/ceph/ceph/pull/47608>`_, Guillaume Abrioux)
* cephadm: fix osd adoption with custom cluster name (`pr#46551 <https://github.com/ceph/ceph/pull/46551>`_, Adam King)
* cephadm: Fix repo_gpgkey should return 2 vars (`pr#47374 <https://github.com/ceph/ceph/pull/47374>`_, Laurent Barbe)
* cephadm: improve message when removing osd (`pr#47071 <https://github.com/ceph/ceph/pull/47071>`_, Guillaume Abrioux)
* cephadm: preserve cephadm user during RPM upgrade (`pr#46790 <https://github.com/ceph/ceph/pull/46790>`_, Scott Shambarger)
* cephadm: reduce spam to cephadm.log (`pr#47313 <https://github.com/ceph/ceph/pull/47313>`_, Adam King)
* cephadm: Remove duplicated process args in promtail and loki (`pr#47654 <https://github.com/ceph/ceph/pull/47654>`_, jinhong.kim)
* cephadm: return nonzero exit code when applying spec fails in bootstrap (`pr#47952 <https://github.com/ceph/ceph/pull/47952>`_, Adam King)
* cephadm: support for Oracle Linux 8 (`pr#47656 <https://github.com/ceph/ceph/pull/47656>`_, Adam King)
* cephfs-shell: move source to separate subdirectory (`pr#47400 <https://github.com/ceph/ceph/pull/47400>`_, Tim Serong)
* cephfs-top: display average read/write/metadata latency (`issue#48619 <http://tracker.ceph.com/issues/48619>`_, `pr#47977 <https://github.com/ceph/ceph/pull/47977>`_, Venky Shankar)
* cephfs-top: fix the rsp/wsp display (`pr#47648 <https://github.com/ceph/ceph/pull/47648>`_, Jos Collin)
* client/fuse: Fix directory DACs overriding for root (`pr#46595 <https://github.com/ceph/ceph/pull/46595>`_, Kotresh HR)
* client: allow overwrites to file with size greater than the max_file_size (`pr#47971 <https://github.com/ceph/ceph/pull/47971>`_, Tamar Shacked)
* client: always return ESTALE directly in handle_reply (`pr#46558 <https://github.com/ceph/ceph/pull/46558>`_, Xiubo Li)
* client: choose auth MDS for getxattr with the Xs caps (`pr#46800 <https://github.com/ceph/ceph/pull/46800>`_, Xiubo Li)
* client: do not release the global snaprealm until unmounting (`pr#46495 <https://github.com/ceph/ceph/pull/46495>`_, Xiubo Li)
* client: Inode::hold_caps_until is time from monotonic clock now (`pr#46563 <https://github.com/ceph/ceph/pull/46563>`_, Laura Flores, Neeraj Pratap Singh)
* client: switch AT_NO_ATTR_SYNC to AT_STATX_DONT_SYNC (`pr#46680 <https://github.com/ceph/ceph/pull/46680>`_, Xiubo Li)
* cmake: disable LTO when building pmdk (`pr#47619 <https://github.com/ceph/ceph/pull/47619>`_, Kefu Chai)
* cmake: pass -Wno-error when building PMDK (`pr#46623 <https://github.com/ceph/ceph/pull/46623>`_, Ilya Dryomov)
* cmake: remove spaces in macro used for compiling cython code (`pr#47483 <https://github.com/ceph/ceph/pull/47483>`_, Kefu Chai)
* cmake: set $PATH for tests using jsonnet tools (`pr#47625 <https://github.com/ceph/ceph/pull/47625>`_, Kefu Chai)
* common/bl: fix FTBFS on C++11 due to C++17's if-with-initializer (`pr#46005 <https://github.com/ceph/ceph/pull/46005>`_, Radosław Zarzyński)
* common/win32,dokan: include bcrypt.h for NTSTATUS (`pr#48016 <https://github.com/ceph/ceph/pull/48016>`_, Lucian Petrut, Kefu Chai)
* common: fix FTBFS due to dout & need_dynamic on GCC-12 (`pr#46214 <https://github.com/ceph/ceph/pull/46214>`_, Radoslaw Zarzynski)
* common: use boost::shared_mutex on Windows (`pr#47493 <https://github.com/ceph/ceph/pull/47493>`_, Lucian Petrut)
* crash: pthread_mutex_lock() (`pr#47683 <https://github.com/ceph/ceph/pull/47683>`_, Patrick Donnelly)
* crimson: fixes for compiling with fmtlib v8 (`pr#47603 <https://github.com/ceph/ceph/pull/47603>`_, Adam C. Emerson, Kefu Chai)
* doc, crimson: document installing crimson with cephadm (`pr#47283 <https://github.com/ceph/ceph/pull/47283>`_, Radoslaw Zarzynski)
* doc/cephadm/services: fix example for specifying rgw placement (`pr#47947 <https://github.com/ceph/ceph/pull/47947>`_, Redouane Kachach)
* doc/cephadm/services: the config section of service specs (`pr#47068 <https://github.com/ceph/ceph/pull/47068>`_, Redouane Kachach)
* doc/cephadm: add note about OSDs being recreated to OSD removal section (`pr#47102 <https://github.com/ceph/ceph/pull/47102>`_, Adam King)
* doc/cephadm: Add post-upgrade section (`pr#47077 <https://github.com/ceph/ceph/pull/47077>`_, Redouane Kachach)
* doc/cephadm: document the new per-fsid cephadm conf location (`pr#47076 <https://github.com/ceph/ceph/pull/47076>`_, Redouane Kachach)
* doc/cephadm: enhancing daemon operations documentation (`pr#47074 <https://github.com/ceph/ceph/pull/47074>`_, Redouane Kachach)
* doc/cephadm: fix example for specifying networks for rgw (`pr#47806 <https://github.com/ceph/ceph/pull/47806>`_, Adam King)
* doc/dev: add context note to dev guide config (`pr#46818 <https://github.com/ceph/ceph/pull/46818>`_, Zac Dover)
* doc/dev: add Dependabot section to essentials.rst (`pr#47042 <https://github.com/ceph/ceph/pull/47042>`_, Zac Dover)
* doc/dev: add IRC registration instructions (`pr#46940 <https://github.com/ceph/ceph/pull/46940>`_, Zac Dover)
* doc/dev: edit delayed-delete.rst (`pr#47051 <https://github.com/ceph/ceph/pull/47051>`_, Zac Dover)
* doc/dev: Elaborate on boost .deb creation (`pr#47415 <https://github.com/ceph/ceph/pull/47415>`_, David Galloway)
* doc/dev: s/github/GitHub/ in essentials.rst (`pr#47048 <https://github.com/ceph/ceph/pull/47048>`_, Zac Dover)
* doc/dev: s/master/main/ essentials.rst dev guide (`pr#46661 <https://github.com/ceph/ceph/pull/46661>`_, Zac Dover)
* doc/dev: s/master/main/ in basic workflow (`pr#46703 <https://github.com/ceph/ceph/pull/46703>`_, Zac Dover)
* doc/dev: s/master/main/ in title (`pr#46721 <https://github.com/ceph/ceph/pull/46721>`_, Zac Dover)
* doc/dev: s/the the/the/ in basic-workflow.rst (`pr#46935 <https://github.com/ceph/ceph/pull/46935>`_, Zac Dover)
* doc/dev_guide: s/master/main in merging.rst (`pr#46709 <https://github.com/ceph/ceph/pull/46709>`_, Zac Dover)
* doc/index.rst: add link to Dev Guide basic workfl (`pr#46904 <https://github.com/ceph/ceph/pull/46904>`_, Zac Dover)
* doc/man/rbd: Mention changed `bluestore_min_alloc_size` (`pr#47579 <https://github.com/ceph/ceph/pull/47579>`_, Niklas Hambüchen)
* doc/mgr: add prompt directives to dashboard.rst (`pr#47822 <https://github.com/ceph/ceph/pull/47822>`_, Zac Dover)
* doc/mgr: edit orchestrator.rst (`pr#47780 <https://github.com/ceph/ceph/pull/47780>`_, Zac Dover)
* doc/mgr: update prompts in dboard.rst includes (`pr#47869 <https://github.com/ceph/ceph/pull/47869>`_, Zac Dover)
* doc/rados/operations: add prompts to operating.rst (`pr#47586 <https://github.com/ceph/ceph/pull/47586>`_, Zac Dover)
* doc/radosgw: Uppercase s3 (`pr#47359 <https://github.com/ceph/ceph/pull/47359>`_, Anthony D'Atri)
* doc/start: alphabetize hardware-recs links (`pr#46339 <https://github.com/ceph/ceph/pull/46339>`_, Zac Dover)
* doc/start: make OSD and MDS structures parallel (`pr#46655 <https://github.com/ceph/ceph/pull/46655>`_, Zac Dover)
* doc/start: Polish network section of hardware-recommendations.rst (`pr#46665 <https://github.com/ceph/ceph/pull/46665>`_, Anthony D'Atri)
* doc/start: rewrite CRUSH para (`pr#46658 <https://github.com/ceph/ceph/pull/46658>`_, Zac Dover)
* doc/start: rewrite hardware-recs networks section (`pr#46652 <https://github.com/ceph/ceph/pull/46652>`_, Zac Dover)
* doc/start: update documenting-ceph branch names (`pr#47955 <https://github.com/ceph/ceph/pull/47955>`_, Zac Dover)
* doc/start: update hardware recs (`pr#47123 <https://github.com/ceph/ceph/pull/47123>`_, Zac Dover)
* doc: update docs for centralized logging (`pr#46946 <https://github.com/ceph/ceph/pull/46946>`_, Aashish Sharma)
* doc: Update release process doc to accurately reflect current process (`pr#47837 <https://github.com/ceph/ceph/pull/47837>`_, David Galloway)
* docs: fix doc link pointing to master in dashboard.rst (`pr#47789 <https://github.com/ceph/ceph/pull/47789>`_, Nizamudeen A)
* exporter: per node metric exporter (`pr#47629 <https://github.com/ceph/ceph/pull/47629>`_, Pere Diaz Bou, Avan Thakkar)
* include/buffer: include <memory> (`pr#47694 <https://github.com/ceph/ceph/pull/47694>`_, Kefu Chai)
* install-deps.sh: do not install libpmem from chacra (`pr#46900 <https://github.com/ceph/ceph/pull/46900>`_, Kefu Chai)
* install-deps: script exit on /ValueError: in centos_stream8 (`pr#47892 <https://github.com/ceph/ceph/pull/47892>`_, Nizamudeen A)
* libcephfs: define AT_NO_ATTR_SYNC back for backward compatibility (`pr#47861 <https://github.com/ceph/ceph/pull/47861>`_, Xiubo Li)
* libcephsqlite: ceph-mgr crashes when compiled with gcc12 (`pr#47270 <https://github.com/ceph/ceph/pull/47270>`_, Ganesh Maharaj Mahalingam)
* librados: rados_ioctx_destroy check for initialized ioctx (`pr#47452 <https://github.com/ceph/ceph/pull/47452>`_, Nitzan Mordechai)
* librbd/cache/pwl: narrow the scope of m_lock in write_image_cache_state() (`pr#47940 <https://github.com/ceph/ceph/pull/47940>`_, Ilya Dryomov, Yin Congmin)
* librbd: bail from schedule_request_lock() if already lock owner (`pr#47162 <https://github.com/ceph/ceph/pull/47162>`_, Christopher Hoffman)
* librbd: retry ENOENT in V2_REFRESH_PARENT as well (`pr#47996 <https://github.com/ceph/ceph/pull/47996>`_, Ilya Dryomov)
* librbd: tweak misleading "image is still primary" error message (`pr#47248 <https://github.com/ceph/ceph/pull/47248>`_, Ilya Dryomov)
* librbd: unlink newest mirror snapshot when at capacity, bump capacity (`pr#46594 <https://github.com/ceph/ceph/pull/46594>`_, Ilya Dryomov)
* librbd: update progress for non-existent objects on deep-copy (`pr#46910 <https://github.com/ceph/ceph/pull/46910>`_, Ilya Dryomov)
* librbd: use actual monitor addresses when creating a peer bootstrap token (`pr#47912 <https://github.com/ceph/ceph/pull/47912>`_, Ilya Dryomov)
* mds: clear MDCache::rejoin\_\*_q queues before recovering file inodes (`pr#46681 <https://github.com/ceph/ceph/pull/46681>`_, Xiubo Li)
* mds: do not assert early on when issuing client leases (`issue#54701 <http://tracker.ceph.com/issues/54701>`_, `pr#46566 <https://github.com/ceph/ceph/pull/46566>`_, Venky Shankar)
* mds: Don't blocklist clients in any replay state (`pr#47110 <https://github.com/ceph/ceph/pull/47110>`_, Kotresh HR)
* mds: fix crash when exporting unlinked dir (`pr#47181 <https://github.com/ceph/ceph/pull/47181>`_, 胡玮文)
* mds: flush mdlog if locked and still has wanted caps not satisfied (`pr#46494 <https://github.com/ceph/ceph/pull/46494>`_, Xiubo Li)
* mds: notify the xattr_version to replica MDSes (`pr#47057 <https://github.com/ceph/ceph/pull/47057>`_, Xiubo Li)
* mds: skip fetching the dirfrags if not a directory (`pr#47432 <https://github.com/ceph/ceph/pull/47432>`_, Xiubo Li)
* mds: standby-replay daemon always removed in MDSMonitor::prepare_beacon (`pr#47281 <https://github.com/ceph/ceph/pull/47281>`_, Patrick Donnelly)
* mds: switch to use projected inode instead (`pr#47058 <https://github.com/ceph/ceph/pull/47058>`_, Xiubo Li)
* mgr, mon: Keep upto date metadata with mgr for MONs (`pr#46559 <https://github.com/ceph/ceph/pull/46559>`_, Laura Flores, Prashant D)
* mgr/cephadm: Add disk rescan feature to the orchestrator (`pr#47311 <https://github.com/ceph/ceph/pull/47311>`_, Adam King, Paul Cuzner)
* mgr/cephadm: add parsing for config on osd specs (`pr#47268 <https://github.com/ceph/ceph/pull/47268>`_, Luis Domingues)
* mgr/cephadm: Adding logic to store grafana cert/key per node (`pr#47950 <https://github.com/ceph/ceph/pull/47950>`_, Redouane Kachach)
* mgr/cephadm: allow binding to loopback for rgw daemons (`pr#47951 <https://github.com/ceph/ceph/pull/47951>`_, Redouane Kachach)
* mgr/cephadm: capture exception when not able to list upgrade tags (`pr#46783 <https://github.com/ceph/ceph/pull/46783>`_, Redouane Kachach)
* mgr/cephadm: check for events key before accessing it (`pr#47317 <https://github.com/ceph/ceph/pull/47317>`_, Redouane Kachach)
* mgr/cephadm: check if a service exists before trying to restart it (`pr#46789 <https://github.com/ceph/ceph/pull/46789>`_, Redouane Kachach)
* mgr/cephadm: clear error message when resuming upgrade (`pr#47373 <https://github.com/ceph/ceph/pull/47373>`_, Adam King)
* mgr/cephadm: don't try to write client/os tuning profiles to known offline hosts (`pr#47953 <https://github.com/ceph/ceph/pull/47953>`_, Adam King)
* mgr/cephadm: fix handling of draining hosts with explicit placement specs (`pr#47657 <https://github.com/ceph/ceph/pull/47657>`_, Adam King)
* mgr/cephadm: Fix how we check if a host belongs to public network (`pr#47946 <https://github.com/ceph/ceph/pull/47946>`_, Redouane Kachach)
* mgr/cephadm: fix the loki address in grafana, promtail configuration files (`pr#47171 <https://github.com/ceph/ceph/pull/47171>`_, jinhong.kim)
* mgr/cephadm: fixing scheduler consistent hashing (`pr#47073 <https://github.com/ceph/ceph/pull/47073>`_, Redouane Kachach)
* mgr/cephadm: limiting ingress/keepalived pass to 8 chars (`pr#47070 <https://github.com/ceph/ceph/pull/47070>`_, Redouane Kachach)
* mgr/cephadm: recreate osd config when redeploy/reconfiguring (`pr#47659 <https://github.com/ceph/ceph/pull/47659>`_, Adam King)
* mgr/cephadm: set dashboard grafana-api-password when user provides one (`pr#47658 <https://github.com/ceph/ceph/pull/47658>`_, Adam King)
* mgr/cephadm: store device info separately from rest of host cache (`pr#46791 <https://github.com/ceph/ceph/pull/46791>`_, Adam King)
* mgr/cephadm: support for miscellaneous config files for daemons (`pr#47312 <https://github.com/ceph/ceph/pull/47312>`_, Adam King)
* mgr/cephadm: support for os tuning profiles (`pr#47316 <https://github.com/ceph/ceph/pull/47316>`_, Adam King)
* mgr/cephadm: try to get FQDN for active instance (`pr#46793 <https://github.com/ceph/ceph/pull/46793>`_, Tatjana Dehler)
* mgr/cephadm: use host shortname for osd memory autotuning (`pr#47075 <https://github.com/ceph/ceph/pull/47075>`_, Adam King)
* mgr/dashboard: Add daemon logs tab to Logs component (`pr#46807 <https://github.com/ceph/ceph/pull/46807>`_, Aashish Sharma)
* mgr/dashboard: add flag to automatically deploy loki/promtail service at bootstrap (`pr#47623 <https://github.com/ceph/ceph/pull/47623>`_, Aashish Sharma)
* mgr/dashboard: add required validation for frontend and monitor port (`pr#47356 <https://github.com/ceph/ceph/pull/47356>`_, Avan Thakkar)
* mgr/dashboard: added pattern validaton for form input (`pr#47329 <https://github.com/ceph/ceph/pull/47329>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: BDD approach for the dashboard cephadm e2e (`pr#46528 <https://github.com/ceph/ceph/pull/46528>`_, Nizamudeen A)
* mgr/dashboard: bump moment from 2.29.1 to 2.29.3 in /src/pybind/mgr/dashboard/frontend (`pr#46718 <https://github.com/ceph/ceph/pull/46718>`_, dependabot[bot])
* mgr/dashboard: bump up teuthology (`pr#47498 <https://github.com/ceph/ceph/pull/47498>`_, Kefu Chai)
* mgr/dashboard: dashboard help command showing wrong syntax for login-banner (`pr#46809 <https://github.com/ceph/ceph/pull/46809>`_, Sarthak0702)
* mgr/dashboard: display helpfull message when the iframe-embedded Grafana dashboard failed to load (`pr#47007 <https://github.com/ceph/ceph/pull/47007>`_, Ngwa Sedrick Meh)
* mgr/dashboard: do not recommend throughput for ssd's only cluster (`pr#47156 <https://github.com/ceph/ceph/pull/47156>`_, Nizamudeen A)
* mgr/dashboard: don't log tracebacks on 404s (`pr#47094 <https://github.com/ceph/ceph/pull/47094>`_, Ernesto Puerta)
* mgr/dashboard: enable addition of custom Prometheus alerts (`pr#47942 <https://github.com/ceph/ceph/pull/47942>`_, Patrick Seidensal)
* mgr/dashboard: ensure limit 0 returns 0 images (`pr#47887 <https://github.com/ceph/ceph/pull/47887>`_, Pere Diaz Bou)
* mgr/dashboard: Feature 54330 osd creation workflow (`pr#46686 <https://github.com/ceph/ceph/pull/46686>`_, Pere Diaz Bou, Nizamudeen A, Sarthak0702)
* mgr/dashboard: fix _rbd_image_refs caching (`pr#47635 <https://github.com/ceph/ceph/pull/47635>`_, Pere Diaz Bou)
* mgr/dashboard: fix nfs exports form issues with squash field (`pr#47961 <https://github.com/ceph/ceph/pull/47961>`_, Nizamudeen A)
* mgr/dashboard: fix unmanaged service creation (`pr#48025 <https://github.com/ceph/ceph/pull/48025>`_, Nizamudeen A)
* mgr/dashboard: grafana frontend e2e testing and update cypress (`pr#47703 <https://github.com/ceph/ceph/pull/47703>`_, Nizamudeen A)
* mgr/dashboard: Hide maintenance option on expand cluster (`pr#47724 <https://github.com/ceph/ceph/pull/47724>`_, Nizamudeen A)
* mgr/dashboard: host list tables doesn't show all services deployed (`pr#47453 <https://github.com/ceph/ceph/pull/47453>`_, Avan Thakkar)
* mgr/dashboard: Improve monitoring tabs content (`pr#46990 <https://github.com/ceph/ceph/pull/46990>`_, Aashish Sharma)
* mgr/dashboard: ingress backend service should list all supported services (`pr#47085 <https://github.com/ceph/ceph/pull/47085>`_, Avan Thakkar)
* mgr/dashboard: iops optimized option enabled (`pr#46819 <https://github.com/ceph/ceph/pull/46819>`_, Pere Diaz Bou)
* mgr/dashboard: iterate through copy of items (`pr#46871 <https://github.com/ceph/ceph/pull/46871>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: prevent alert redirect (`pr#47146 <https://github.com/ceph/ceph/pull/47146>`_, Tatjana Dehler)
* mgr/dashboard: rbd image pagination (`pr#47104 <https://github.com/ceph/ceph/pull/47104>`_, Pere Diaz Bou, Nizamudeen A)
* mgr/dashboard: rbd striping setting pre-population and pop-over (`pr#47409 <https://github.com/ceph/ceph/pull/47409>`_, Vrushal Chaudhari)
* mgr/dashboard: rbd-mirror batch backport (`pr#46532 <https://github.com/ceph/ceph/pull/46532>`_, Pedro Gonzalez Gomez, Pere Diaz Bou, Nizamudeen A, Melissa Li, Sarthak0702, Avan Thakkar, Aashish Sharma)
* mgr/dashboard: remove token logging (`pr#47430 <https://github.com/ceph/ceph/pull/47430>`_, Pere Diaz Bou)
* mgr/dashboard: Show error on creating service with duplicate service id (`pr#47403 <https://github.com/ceph/ceph/pull/47403>`_, Aashish Sharma)
* mgr/dashboard: stop polling when page is not visible (`pr#46672 <https://github.com/ceph/ceph/pull/46672>`_, Sarthak0702)
* mgr/dashboard:Get different storage class metrics in Prometheus dashboard (`pr#47201 <https://github.com/ceph/ceph/pull/47201>`_, Aashish Sharma)
* mgr/nfs: validate virtual_ip parameter (`pr#46794 <https://github.com/ceph/ceph/pull/46794>`_, Redouane Kachach)
* mgr/orchestrator/tests: don't match exact whitespace in table output (`pr#47858 <https://github.com/ceph/ceph/pull/47858>`_, Adam King)
* mgr/rook: fix error when trying to get the list of nfs services `pr#48199 <https://github.com/ceph/ceph/pull/48199>`_, Juan Miguel Olmo)
* mgr/snap_schedule: replace .snap with the client configured snap dir name (`pr#47734 <https://github.com/ceph/ceph/pull/47734>`_, Milind Changire, Venky Shankar, Neeraj Pratap Singh)
* mgr/snap_schedule: Use rados.Ioctx.remove_object() instead of remove() (`pr#48013 <https://github.com/ceph/ceph/pull/48013>`_, Andreas Teuchert)
* mgr/telemetry: add `perf_memory_metrics` collection to telemetry (`pr#47826 <https://github.com/ceph/ceph/pull/47826>`_, Laura Flores)
* mgr/telemetry: handle daemons with complex ids (`pr#48283 <https://github.com/ceph/ceph/pull/48283>`_, Laura Flores)
* mgr/telemetry: reset health warning after re-opting-in (`pr#47289 <https://github.com/ceph/ceph/pull/47289>`_, Yaarit Hatuka)
* mgr/volumes: add interface to check the presence of subvolumegroups/subvolumes (`pr#47474 <https://github.com/ceph/ceph/pull/47474>`_, Neeraj Pratap Singh)
* mgr/volumes: Add volume info command (`pr#47768 <https://github.com/ceph/ceph/pull/47768>`_, Neeraj Pratap Singh)
* mgr/volumes: Few mgr volumes backports (`pr#47894 <https://github.com/ceph/ceph/pull/47894>`_, Rishabh Dave, Kotresh HR, Nikhilkumar Shelke)
* mgr/volumes: filter internal directories in 'subvolumegroup ls' command (`pr#47511 <https://github.com/ceph/ceph/pull/47511>`_, Nikhilkumar Shelke)
* mgr/volumes: Fix subvolume creation in FIPS enabled system (`pr#47368 <https://github.com/ceph/ceph/pull/47368>`_, Kotresh HR)
* mgr/volumes: prevent intermittent ParsingError failure in "clone cancel" (`pr#47747 <https://github.com/ceph/ceph/pull/47747>`_, John Mulligan)
* mgr/volumes: remove incorrect 'size' from output of 'snapshot info' (`pr#46804 <https://github.com/ceph/ceph/pull/46804>`_, Nikhilkumar Shelke)
* mgr/volumes: subvolume ls command crashes if groupname as '_nogroup' (`pr#46805 <https://github.com/ceph/ceph/pull/46805>`_, Nikhilkumar Shelke)
* mgr/volumes: subvolumegroup quotas (`pr#46667 <https://github.com/ceph/ceph/pull/46667>`_, Kotresh HR)
* mgr: Define PY_SSIZE_T_CLEAN ahead of every Python.h (`pr#47616 <https://github.com/ceph/ceph/pull/47616>`_, Pete Zaitcev, Kefu Chai)
* mgr: relax "pending_service_map.epoch > service_map.epoch" assert (`pr#46738 <https://github.com/ceph/ceph/pull/46738>`_, Mykola Golub)
* mirror snapshot schedule and trash purge schedule fixes (`pr#46781 <https://github.com/ceph/ceph/pull/46781>`_, Ilya Dryomov)
* mon/ConfigMonitor: fix config get key with whitespace (`pr#47381 <https://github.com/ceph/ceph/pull/47381>`_, Nitzan Mordechai)
* mon/Elector: notify_rank_removed erase rank from both live_pinging and dead_pinging sets for highest ranked MON (`pr#47086 <https://github.com/ceph/ceph/pull/47086>`_, Kamoltat)
* mon/MDSMonitor: fix standby-replay mds being removed from MDSMap unexpectedly (`pr#47902 <https://github.com/ceph/ceph/pull/47902>`_, 胡玮文)
* mon/OSDMonitor: Ensure kvmon() is writeable before handling "osd new" cmd (`pr#46689 <https://github.com/ceph/ceph/pull/46689>`_, Sridhar Seshasayee)
* monitoring/ceph-mixin: OSD overview typo fix (`pr#47387 <https://github.com/ceph/ceph/pull/47387>`_, Tatjana Dehler)
* monitoring: ceph mixin backports (`pr#47867 <https://github.com/ceph/ceph/pull/47867>`_, Aswin Toni, Arthur Outhenin-Chalandre, Anthony D'Atri, Tatjana Dehler)
* msg: fix deadlock when handling existing but closed v2 connection (`pr#47930 <https://github.com/ceph/ceph/pull/47930>`_, Radosław Zarzyński)
* msg: Fix Windows IPv6 support (`pr#47302 <https://github.com/ceph/ceph/pull/47302>`_, Lucian Petrut)
* msg: Log at higher level when Throttle::get_or_fail() fails (`pr#47765 <https://github.com/ceph/ceph/pull/47765>`_, Brad Hubbard)
* msg: reset ProtocolV2's frame assembler in appropriate thread (`pr#47931 <https://github.com/ceph/ceph/pull/47931>`_, Radoslaw Zarzynski)
* os/bluestore: fix AU accounting in bluestore_cache_other mempool (`pr#47339 <https://github.com/ceph/ceph/pull/47339>`_, Igor Fedotov)
* os/bluestore: Fix collision between BlueFS and BlueStore deferred writes (`pr#47297 <https://github.com/ceph/ceph/pull/47297>`_, Adam Kupczyk)
* osd, mds: fix the "heap" admin cmd printing always to error stream (`pr#47825 <https://github.com/ceph/ceph/pull/47825>`_, Radoslaw Zarzynski)
* osd, tools, kv: non-aggressive, on-line trimming of accumulated dups (`pr#47688 <https://github.com/ceph/ceph/pull/47688>`_, Radoslaw Zarzynski, Nitzan Mordechai)
* osd/scrub: do not start scrubbing if the PG is snap-trimming (`pr#46498 <https://github.com/ceph/ceph/pull/46498>`_, Ronen Friedman)
* osd/scrub: late-arriving reservation grants are not an error (`pr#46872 <https://github.com/ceph/ceph/pull/46872>`_, Ronen Friedman)
* osd/scrub: Reintroduce scrub starts message (`pr#47621 <https://github.com/ceph/ceph/pull/47621>`_, Prashant D)
* osd/scrubber/pg_scrubber.cc: fix bug where scrub machine gets stuck (`pr#46844 <https://github.com/ceph/ceph/pull/46844>`_, Cory Snyder)
* osd/SnapMapper: fix legacy key conversion in snapmapper class (`pr#47133 <https://github.com/ceph/ceph/pull/47133>`_, Manuel Lausch, Matan Breizman)
* osd: Handle oncommits and wait for future work items from mClock queue (`pr#47490 <https://github.com/ceph/ceph/pull/47490>`_, Sridhar Seshasayee)
* osd: return ENOENT if pool information is invalid during tier-flush (`pr#47929 <https://github.com/ceph/ceph/pull/47929>`_, Myoungwon Oh)
* osd: Set initial mClock QoS params at CONF_DEFAULT level (`pr#47020 <https://github.com/ceph/ceph/pull/47020>`_, Sridhar Seshasayee)
* PendingReleaseNotes: Note the fix for high CPU utilization during recovery (`pr#48004 <https://github.com/ceph/ceph/pull/48004>`_, Sridhar Seshasayee)
* pybind/mgr/cephadm/serve: don't remove ceph.conf which leads to qa failure (`pr#47072 <https://github.com/ceph/ceph/pull/47072>`_, Dhairya Parmar)
* pybind/mgr/dashboard: do not use distutils.version.StrictVersion (`pr#47602 <https://github.com/ceph/ceph/pull/47602>`_, Kefu Chai)
* pybind/mgr/pg_autoscaler: change overlapping roots to warning (`pr#47519 <https://github.com/ceph/ceph/pull/47519>`_, Kamoltat)
* pybind/mgr: ceph osd status crash with ZeroDivisionError (`pr#46697 <https://github.com/ceph/ceph/pull/46697>`_, Nitzan Mordechai)
* pybind/mgr: fix flake8 (`pr#47391 <https://github.com/ceph/ceph/pull/47391>`_, Avan Thakkar)
* python-common: allow crush device class to be set from osd service spec (`pr#46792 <https://github.com/ceph/ceph/pull/46792>`_, Cory Snyder)
* qa/cephadm: specify using container host distros for workunits (`pr#47910 <https://github.com/ceph/ceph/pull/47910>`_, Adam King)
* qa/cephfs: fallback to older way of get_op_read_count (`pr#46899 <https://github.com/ceph/ceph/pull/46899>`_, Dhairya Parmar)
* qa/suites/rbd/pwl-cache: ensure recovery is actually tested (`pr#47129 <https://github.com/ceph/ceph/pull/47129>`_, Ilya Dryomov, Yin Congmin)
* qa/suites/rbd: disable workunit timeout for dynamic_features_no_cache (`pr#47159 <https://github.com/ceph/ceph/pull/47159>`_, Ilya Dryomov)
* qa/suites/rbd: place cache file on tmpfs for xfstests (`pr#46598 <https://github.com/ceph/ceph/pull/46598>`_, Ilya Dryomov)
* qa/tasks/ceph_manager.py: increase test_pool_min_size timeout (`pr#47445 <https://github.com/ceph/ceph/pull/47445>`_, Kamoltat)
* qa/workunits/cephadm: update test_repos master -> main (`pr#47315 <https://github.com/ceph/ceph/pull/47315>`_, Adam King)
* qa: wait rank 0 to become up:active state before mounting fuse client (`pr#46801 <https://github.com/ceph/ceph/pull/46801>`_, Xiubo Li)
* quincy -- sse s3 changes (`pr#46467 <https://github.com/ceph/ceph/pull/46467>`_, Casey Bodley, Marcus Watts, Priya Sehgal)
* rbd-fuse: librados will filter out -r option from command-line (`pr#46954 <https://github.com/ceph/ceph/pull/46954>`_, wanwencong)
* rbd-mirror: don't prune non-primary snapshot when restarting delta sync (`pr#46591 <https://github.com/ceph/ceph/pull/46591>`_, Ilya Dryomov)
* rbd-mirror: generally skip replay/resync if remote image is not primary (`pr#46814 <https://github.com/ceph/ceph/pull/46814>`_, Ilya Dryomov)
* rbd-mirror: remove bogus completed_non_primary_snapshots_exist check (`pr#47126 <https://github.com/ceph/ceph/pull/47126>`_, Ilya Dryomov)
* rbd-mirror: resume pending shutdown on error in snapshot replayer (`pr#47914 <https://github.com/ceph/ceph/pull/47914>`_, Ilya Dryomov)
* rbd: don't default empty pool name unless namespace is specified (`pr#47144 <https://github.com/ceph/ceph/pull/47144>`_, Ilya Dryomov)
* rbd: find_action() should sort actions first (`pr#47584 <https://github.com/ceph/ceph/pull/47584>`_, Ilya Dryomov)
* RGW - Swift retarget needs bucket set on object (`pr#46719 <https://github.com/ceph/ceph/pull/46719>`_, Daniel Gryniewicz)
* rgw/backport/quincy: Fix crashes with Sync policy APIs (`pr#47993 <https://github.com/ceph/ceph/pull/47993>`_, Soumya Koduri)
* rgw/dbstore: Fix build errors on centos9 (`pr#46915 <https://github.com/ceph/ceph/pull/46915>`_, Soumya Koduri)
* rgw: Avoid segfault when OPA authz is enabled (`pr#46107 <https://github.com/ceph/ceph/pull/46107>`_, Benoît Knecht)
* rgw: better tenant id from the uri on anonymous access (`pr#47342 <https://github.com/ceph/ceph/pull/47342>`_, Rafał Wądołowski, Marcus Watts)
* rgw: check object storage_class when check_disk_state (`pr#46580 <https://github.com/ceph/ceph/pull/46580>`_, Huber-ming)
* rgw: data sync uses yield_spawn_window() (`pr#45714 <https://github.com/ceph/ceph/pull/45714>`_, Casey Bodley)
* rgw: Fix data race in ChangeStatus (`pr#47195 <https://github.com/ceph/ceph/pull/47195>`_, Adam C. Emerson)
* rgw: Guard against malformed bucket URLs (`pr#47191 <https://github.com/ceph/ceph/pull/47191>`_, Adam C. Emerson)
* rgw: log access key id in ops logs (`pr#46624 <https://github.com/ceph/ceph/pull/46624>`_, Cory Snyder)
* rgw: reopen ops log file on sighup (`pr#46625 <https://github.com/ceph/ceph/pull/46625>`_, Cory Snyder)
* rgw_rest_user_policy: Fix GetUserPolicy & ListUserPolicies responses (`pr#47235 <https://github.com/ceph/ceph/pull/47235>`_, Sumedh A. Kulkarni)
* rgwlc: fix segfault resharding during lc (`pr#46742 <https://github.com/ceph/ceph/pull/46742>`_, Mark Kogan)
* script/build-integration-branch: add quincy to the list of releases (`pr#46361 <https://github.com/ceph/ceph/pull/46361>`_, Yuri Weinstein)
* SimpleRADOSStriper: Avoid moving bufferlists by using deque in read() (`pr#47909 <https://github.com/ceph/ceph/pull/47909>`_, Matan Breizman)
* src/mgr/DaemonServer.cc: fix typo in output gap >= max_pg_num_change (`pr#47210 <https://github.com/ceph/ceph/pull/47210>`_, Kamoltat)
* test/lazy-omap-stats: Various enhancements (`pr#47932 <https://github.com/ceph/ceph/pull/47932>`_, Brad Hubbard)
* test/{librbd, rgw}: increase delay between and number of bind attempts (`pr#48023 <https://github.com/ceph/ceph/pull/48023>`_, Ilya Dryomov)
* test/{librbd, rgw}: retry when bind fail with port 0 (`pr#47980 <https://github.com/ceph/ceph/pull/47980>`_, Kefu Chai)
* tooling: Change mrun to use bash (`pr#46076 <https://github.com/ceph/ceph/pull/46076>`_, Adam C. Emerson)
* tools: ceph-objectstore-tool is able to trim pg log dups' entries (`pr#46706 <https://github.com/ceph/ceph/pull/46706>`_, Radosław Zarzyński)
* win32_deps_build.sh: master -> main for wnbd (`pr#46763 <https://github.com/ceph/ceph/pull/46763>`_, Ilya Dryomov)

v17.2.3 Quincy
==============

This is a hotfix release that addresses a libcephsqlite crash in the mgr.

Notable Changes
---------------
* A libcephsqlite bug that caused the mgr to crash repeatedly and die is now
  fixed. The bug was exposed due to 17.2.2 being built with gcc 8.5.0-14, which contains
  a new patch to check for invalid regex. 17.2.1 was built using gcc 8.5.0-13, which
  does not contain the invalid regex patch.

  Relevant tracker: https://tracker.ceph.com/issues/55304

  Relevant BZ: https://bugzilla.redhat.com/show_bug.cgi?id=2110797

Changelog
---------

* libcephsqlite: ceph-mgr crashes when compiled with gcc12 (`pr#47270 <https://github.com/ceph/ceph/pull/47270>`_, Ganesh Maharaj Mahalingam)

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
   but recommended.):

   .. prompt:: bash #

      ceph osd set noout

#. Upgrade monitors by installing the new packages and restarting the
   monitor daemons.  For example, on each monitor host,:

   .. prompt:: bash #

      systemctl restart ceph-mon.target

   Once all monitors are up, verify that the monitor upgrade is
   complete by looking for the ``quincy`` string in the mon
   map.  The command:

   .. prompt:: bash #

      ceph mon dump | grep min_mon_release

   should report::

     min_mon_release 17 (quincy)

   If it doesn't, that implies that one or more monitors hasn't been
   upgraded and restarted and/or the quorum does not include all monitors.

#. Upgrade ``ceph-mgr`` daemons by installing the new packages and
   restarting all manager daemons.  For example, on each manager host,:

   .. prompt:: bash #

      systemctl restart ceph-mgr.target

   Verify the ``ceph-mgr`` daemons are running by checking ``ceph
   -s``:

   .. prompt:: bash #

      ceph -s

   ::

     ...
       services:
        mon: 3 daemons, quorum foo,bar,baz
        mgr: foo(active), standbys: bar, baz
     ...

#. Upgrade all OSDs by installing the new packages and restarting the
   ceph-osd daemons on all OSD hosts:

   .. prompt:: bash #

      systemctl restart ceph-osd.target

#. Upgrade all CephFS MDS daemons. For each CephFS file system,

   #. Disable standby_replay.  Before executing, note the current value
      so that it may be re-enabled after the upgrade (if currently enabled):

      .. prompt:: bash #

	 ceph fs get <fs_name> | grep allow_standby_replay
	 ceph fs set <fs_name> allow_standby_replay false

   #. Reduce the number of ranks to 1.  (Make note of the original
      number of MDS daemons first if you plan to restore it later.):

      .. prompt:: bash #

	 ceph fs status
	 ceph fs set <fs_name> max_mds 1

   #. Wait for the cluster to deactivate any non-zero ranks by
      periodically checking the status:

      .. prompt:: bash #

	 ceph fs status

   #. Take all standby MDS daemons offline on the appropriate hosts with:

      .. prompt:: bash #

	 systemctl stop ceph-mds@<daemon_name>

   #. Confirm that only one MDS is online and is rank 0 for your FS:

      .. prompt:: bash #

	 ceph fs status

   #. Upgrade the last remaining MDS daemon by installing the new
      packages and restarting the daemon:

      .. prompt:: bash #

         systemctl restart ceph-mds.target

   #. Restart all standby MDS daemons that were taken offline:

      .. prompt:: bash #

	 systemctl start ceph-mds.target

   #. Restore the original value of ``max_mds`` for the volume:

      .. prompt:: bash #

	 ceph fs set <fs_name> max_mds <original_max_mds>

    #. Restore the original value of ``allow_standby_replay`` for the volume if
       it was ``true``:

      .. prompt:: bash #

	 ceph fs set <fs_name> allow_standby_replay true

#. Upgrade all radosgw daemons by upgrading packages and restarting
   daemons on all hosts:

   .. prompt:: bash #

      systemctl restart ceph-radosgw.target

#. Complete the upgrade by disallowing pre-Quincy OSDs and enabling
   all new Quincy-only functionality:

   .. prompt:: bash #

      ceph osd require-osd-release quincy

#. If you set ``noout`` at the beginning, be sure to clear it with:

   .. prompt:: bash #

      ceph osd unset noout

#. Consider transitioning your cluster to use the cephadm deployment
   and orchestration framework to simplify cluster management and
   future upgrades.  For more information on converting an existing
   cluster to cephadm, see :ref:`cephadm-adoption`.

Post-upgrade
~~~~~~~~~~~~

#. Verify the cluster is healthy with ``ceph health``. If your cluster is
   running Filestore, a deprecation warning is expected. This warning can
   be temporarily muted using the following command:

   .. prompt:: bash #

      ceph health mute OSD_FILESTORE

#. If you are upgrading from Mimic, or did not already do so when you
   upgraded to Nautilus, we recommend you enable the new :ref:`v2
   network protocol <msgr2>`, issue the following command:

   .. prompt:: bash #

      ceph mon enable-msgr2

   This will instruct all monitors that bind to the old default port
   6789 for the legacy v1 protocol to also bind to the new 3300 v2
   protocol port.  To see if all monitors have been updated, run this:

   .. prompt:: bash #

      ceph mon dump

   and verify that each monitor has both a ``v2:`` and ``v1:`` address
   listed.

#. Consider enabling the :ref:`telemetry module <telemetry>` to send
   anonymized usage statistics and crash information to the Ceph
   upstream developers.  To see what would be reported (without actually
   sending any information to anyone),:

   .. prompt:: bash #

      ceph telemetry preview-all

   If you are comfortable with the data that is reported, you can opt-in to
   automatically report the high-level cluster metadata with:

   .. prompt:: bash #

      ceph telemetry on

   The public dashboard that aggregates Ceph telemetry can be found at
   `https://telemetry-public.ceph.com/ <https://telemetry-public.ceph.com/>`_.

   For more information about the telemetry module, see :ref:`the
   documentation <telemetry>`.


Upgrading from pre-Octopus releases (like Nautilus)
---------------------------------------------------


You *must* first upgrade to Octopus (15.2.z) or Pacific (16.2.z) before
upgrading to Quincy.
