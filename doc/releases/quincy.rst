======
Quincy
======

Quincy is the 17th stable release of Ceph.  It is named after Squidward
Quincy Tentacles from Spongebob Squarepants.


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
* ceph-exporter: cephadm changes (`pr#49771 <https://github.com/ceph/ceph/pull/49771>`_, Avan Thakkar)
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
* qa/fs/mixed-clients: specify distros for tests (`pr#49957 <https://github.com/ceph/ceph/pull/49957>`_, Dhairya Parmar)
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
