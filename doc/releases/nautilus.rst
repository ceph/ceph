v14.2.10 Nautilus
=================

This is the tenth release in the Nautilus series. In addition to fixing
a security-related bug in RGW, this release brings a number of bugfixes
across all major components of Ceph. We recommend that all Nautilus users
upgrade to this release.

Notable Changes
---------------

* CVE-2020-10753: rgw: sanitize newlines in s3 CORSConfiguration's ExposeHeader
  (William Bowling, Adam Mohammed, Casey Bodley)

* RGW: Bucket notifications now support Kafka endpoints. This requires librdkafka of
  version 0.9.2 and up. Note that Ubuntu 16.04.6 LTS (Xenial Xerus) has an older
  version of librdkafka, and would require an update to the library.

* The pool parameter ``target_size_ratio``, used by the pg autoscaler,
  has changed meaning. It is now normalized across pools, rather than
  specifying an absolute ratio. For details, see :ref:`pg-autoscaler`.
  If you have set target size ratios on any pools, you may want to set
  these pools to autoscale ``warn`` mode to avoid data movement during
  the upgrade::

    ceph osd pool set <pool-name> pg_autoscale_mode warn

* The behaviour of the ``-o`` argument to the rados tool has been reverted to
  its orignal behaviour of indicating an output file. This reverts it to a more
  consistent behaviour when compared to other tools. Specifying object size is now
  accomplished by using an upper case O ``-O``.

* The format of MDSs in `ceph fs dump` has changed.

* Ceph will issue a health warning if a RADOS pool's ``size`` is set to 1
  or in other words the pool is configured with no redundancy. This can
  be fixed by setting the pool size to the minimum recommended value
  with::

    ceph osd pool set <pool-name> size <num-replicas>

  The warning can be silenced with::

    ceph config set global mon_warn_on_pool_no_redundancy false

* RGW: bucket listing performance on sharded bucket indexes has been
  notably improved by heuristically -- and significantly, in many
  cases -- reducing the number of entries requested from each bucket
  index shard.

Changelog
---------

* build/ops: address SElinux denials observed in rgw/multisite test run (`pr#34539 <https://github.com/ceph/ceph/pull/34539>`_, Kefu Chai, Kaleb S. Keithley)
* build/ops: ceph.spec.in: build on el8 (`pr#35599 <https://github.com/ceph/ceph/pull/35599>`_, Kefu Chai, Brad Hubbard, Alfonso Martínez, Nathan Cutler, Sage Weil, luo.runbing)
* build/ops: cmake: Improve test for 16-byte atomic support on IBM Z (`pr#33716 <https://github.com/ceph/ceph/pull/33716>`_, Ulrich Weigand)
* build/ops: do_cmake.sh: fix application of -DWITH_RADOSGW_KAFKA_ENDPOINT=OFF (`pr#34008 <https://github.com/ceph/ceph/pull/34008>`_, Nathan Cutler, Kefu Chai)
* build/ops: install-deps.sh: Use dnf for rhel/centos 8 (`pr#35461 <https://github.com/ceph/ceph/pull/35461>`_, Brad Hubbard)
* build/ops: rpm: add python3-saml as install dependency (`pr#34475 <https://github.com/ceph/ceph/pull/34475>`_, Kefu Chai, Ernesto Puerta)
* build/ops: selinux: Allow ceph to setsched (`pr#34433 <https://github.com/ceph/ceph/pull/34433>`_, Brad Hubbard)
* build/ops: selinux: Allow ceph-mgr access to httpd dir (`pr#34434 <https://github.com/ceph/ceph/pull/34434>`_, Brad Hubbard)
* build/ops: selinux: Allow getattr access to /proc/kcore (`pr#34870 <https://github.com/ceph/ceph/pull/34870>`_, Brad Hubbard)
* build/ops: spec: address some warnings raised by RPM 4.15.1 (`pr#34527 <https://github.com/ceph/ceph/pull/34527>`_, Nathan Cutler)
* ceph-volume/batch: check lvs list before access (`pr#34481 <https://github.com/ceph/ceph/pull/34481>`_, Jan Fajerski)
* ceph-volume/batch: return success when all devices are filtered (`pr#34478 <https://github.com/ceph/ceph/pull/34478>`_, Jan Fajerski)
* ceph-volume: add and delete lvm tags in a single lvchange call (`pr#35453 <https://github.com/ceph/ceph/pull/35453>`_, Jan Fajerski)
* ceph-volume: add ceph.osdspec_affinity tag (`pr#35132 <https://github.com/ceph/ceph/pull/35132>`_, Joshua Schmid)
* ceph-volume: devices/simple/scan: Fix string in log statement (`pr#34445 <https://github.com/ceph/ceph/pull/34445>`_, Jan Fajerski)
* ceph-volume: fix nautilus functional tests (`pr#33391 <https://github.com/ceph/ceph/pull/33391>`_, Jan Fajerski)
* ceph-volume: lvm: get_device_vgs() filter by provided prefix (`pr#33616 <https://github.com/ceph/ceph/pull/33616>`_, Jan Fajerski, Yehuda Sadeh)
* ceph-volume: prepare: use \*-slots arguments for implicit sizing (`pr#34278 <https://github.com/ceph/ceph/pull/34278>`_, Jan Fajerski)
* ceph-volume: silence 'ceph-bluestore-tool' failures (`pr#33428 <https://github.com/ceph/ceph/pull/33428>`_, Sébastien Han)
* ceph-volume: strip _dmcrypt suffix in simple scan json output (`pr#33722 <https://github.com/ceph/ceph/pull/33722>`_, Jan Fajerski)
* cephfs/tools: add accounted_rstat/rstat when building file dentry (`pr#35185 <https://github.com/ceph/ceph/pull/35185>`_, Xiubo Li)
* cephfs/tools: cephfs-journal-tool: correctly parse --dry_run argument (`pr#34784 <https://github.com/ceph/ceph/pull/34784>`_, Milind Changire)
* cephfs: allow pool names with hyphen and period (`pr#35391 <https://github.com/ceph/ceph/pull/35391>`_, Rishabh Dave, Ramana Raja)
* cephfs: ceph-fuse: link to libfuse3 and pass "-o big_writes" to libfuse if libfuse < 3.0.0 (`pr#34771 <https://github.com/ceph/ceph/pull/34771>`_, Kefu Chai, Xiubo Li, "Yan, Zheng")
* cephfs: client: expose Client::ll_register_callback via libcephfs (`pr#35393 <https://github.com/ceph/ceph/pull/35393>`_, Kefu Chai, Jeff Layton)
* cephfs: client: fix Finisher assert failure (`pr#35000 <https://github.com/ceph/ceph/pull/35000>`_, Xiubo Li)
* cephfs: client: fix bad error handling in lseek SEEK_HOLE / SEEK_DATA (`pr#34308 <https://github.com/ceph/ceph/pull/34308>`_, Jeff Layton)
* cephfs: client: only set MClientCaps::FLAG_SYNC when flushing dirty auth caps (`pr#35118 <https://github.com/ceph/ceph/pull/35118>`_, Jeff Layton)
* cephfs: client: reset requested_max_size if file write is not wanted (`pr#34767 <https://github.com/ceph/ceph/pull/34767>`_, "Yan, Zheng")
* cephfs: mds: Handle blacklisted error in purge queue (`pr#35149 <https://github.com/ceph/ceph/pull/35149>`_, Varsha Rao)
* cephfs: mds: SIGSEGV in Migrator::export_sessions_flushed (`pr#33751 <https://github.com/ceph/ceph/pull/33751>`_, "Yan, Zheng")
* cephfs: mds: Using begin() and empty() to iterate the xlist (`pr#34338 <https://github.com/ceph/ceph/pull/34338>`_, Shen Hang, "Yan, Zheng")
* cephfs: mds: add configurable snapshot limit (`pr#33295 <https://github.com/ceph/ceph/pull/33295>`_, Milind Changire)
* cephfs: mds: display scrub status in ceph status (`issue#41508 <http://tracker.ceph.com/issues/41508>`_, `issue#42713 <http://tracker.ceph.com/issues/42713>`_, `issue#44520 <http://tracker.ceph.com/issues/44520>`_, `issue#42168 <http://tracker.ceph.com/issues/42168>`_, `issue#42169 <http://tracker.ceph.com/issues/42169>`_, `issue#42569 <http://tracker.ceph.com/issues/42569>`_, `issue#41424 <http://tracker.ceph.com/issues/41424>`_, `issue#42835 <http://tracker.ceph.com/issues/42835>`_, `issue#36370 <http://tracker.ceph.com/issues/36370>`_, `issue#42325 <http://tracker.ceph.com/issues/42325>`_, `pr#30704 <https://github.com/ceph/ceph/pull/30704>`_, Venky Shankar, Patrick Donnelly, Sage Weil, Kefu Chai)
* cephfs: mds: don't shallow copy when decoding xattr map (`pr#35199 <https://github.com/ceph/ceph/pull/35199>`_, "Yan, Zheng")
* cephfs: mds: handle bad purge queue item encoding (`pr#34307 <https://github.com/ceph/ceph/pull/34307>`_, "Yan, Zheng")
* cephfs: mds: handle ceph_assert on blacklisting (`pr#34435 <https://github.com/ceph/ceph/pull/34435>`_, Milind Changire)
* cephfs: mds: just delete MDSIOContextBase during shutdown (`pr#34343 <https://github.com/ceph/ceph/pull/34343>`_, "Yan, Zheng", Patrick Donnelly)
* cephfs: mds: take xlock in the order requests start locking (`pr#35392 <https://github.com/ceph/ceph/pull/35392>`_, "Yan, Zheng")
* common/bl: fix memory corruption in bufferlist::claim_append() (`pr#34516 <https://github.com/ceph/ceph/pull/34516>`_, Radoslaw Zarzynski)
* common/blkdev: compilation of telemetry and device backports (`pr#33726 <https://github.com/ceph/ceph/pull/33726>`_, Sage Weil, Difan Zhang, Patrick Seidensal, Kefu Chai)
* common/blkdev: fix some problems with smart scraping (`pr#33421 <https://github.com/ceph/ceph/pull/33421>`_, Sage Weil)
* common/ceph_time: tolerate mono time going backwards (`pr#34542 <https://github.com/ceph/ceph/pull/34542>`_, Sage Weil)
* common/options: Disable bluefs_buffered_io by default again (`pr#34297 <https://github.com/ceph/ceph/pull/34297>`_, Mark Nelson)
* compressor/lz4: work around bug in liblz4 versions <1.8.2 (`pr#35004 <https://github.com/ceph/ceph/pull/35004>`_, Sage Weil, Dan van der Ster)
* core: bluestore/bdev: initialize size when creating object (`pr#34832 <https://github.com/ceph/ceph/pull/34832>`_, Willem Jan Withagen)
* core: bluestore: Don't pollute old journal when add new device (`pr#34796 <https://github.com/ceph/ceph/pull/34796>`_, Yang Honggang)
* core: bluestore: fix 'unused' calculation (`pr#34794 <https://github.com/ceph/ceph/pull/34794>`_, xie xingguo, Igor Fedotov)
* core: bluestore: fix extent leak after main device expand (`pr#34711 <https://github.com/ceph/ceph/pull/34711>`_, Igor Fedotov)
* core: bluestore: more flexible DB volume space usage (`pr#33889 <https://github.com/ceph/ceph/pull/33889>`_, Igor Fedotov)
* core: bluestore: open DB in read-only when expanding DB/WAL (`pr#34611 <https://github.com/ceph/ceph/pull/34611>`_, Igor Fedotov, Jianpeng Ma, Adam Kupczyk)
* core: bluestore: prevent BlueFS::dirty_files from being leaked when syncing metadata (`pr#34515 <https://github.com/ceph/ceph/pull/34515>`_, Xuehan Xu)
* core: msg/async/rdma: fix bug event center is blocked by rdma construct connection for transport ib sync msg (`pr#34780 <https://github.com/ceph/ceph/pull/34780>`_, Peng Liu)
* core: msgr: backport the EventCenter-related fixes (`pr#33820 <https://github.com/ceph/ceph/pull/33820>`_, Radoslaw Zarzynski, Jeff Layton, Kefu Chai)
* core: rados: prevent ShardedOpWQ suicide_grace drop when waiting for work (`pr#34882 <https://github.com/ceph/ceph/pull/34882>`_, Dan Hill)
* doc/mgr/telemetry: added device channel details (`pr#33684 <https://github.com/ceph/ceph/pull/33684>`_, Yaarit Hatuka)
* doc/releases/nautilus: restart OSDs to make them bind to v2 addr (`pr#34524 <https://github.com/ceph/ceph/pull/34524>`_, Nathan Cutler)
* doc: fix parameter to set pg autoscale mode (`pr#34518 <https://github.com/ceph/ceph/pull/34518>`_, Changcheng Liu)
* doc: mds-config-ref: update 'mds_log_max_segments' value (`pr#35278 <https://github.com/ceph/ceph/pull/35278>`_, Konstantin Shalygin)
* doc: reset PendingReleaseNotes following 14.2.8 release (`pr#33863 <https://github.com/ceph/ceph/pull/33863>`_, Nathan Cutler)
* global: ensure CEPH_ARGS is decoded before early arg processing (`pr#33261 <https://github.com/ceph/ceph/pull/33261>`_, Kefu Chai, Jason Dillaman)
* mgr/DaemonServer: fix pg merge checks (`pr#34354 <https://github.com/ceph/ceph/pull/34354>`_, Sage Weil)
* mgr/PyModule: fix missing tracebacks in handle_pyerror() (`pr#34627 <https://github.com/ceph/ceph/pull/34627>`_, Tim Serong)
* mgr/balancer: tolerate pgs outside of target weight map (`pr#34761 <https://github.com/ceph/ceph/pull/34761>`_, Sage Weil)
* mgr/dashboard/grafana: Add rbd-image details dashboard (`pr#35248 <https://github.com/ceph/ceph/pull/35248>`_, Enno Gotthold)
* mgr/dashboard: 'destroyed' view in CRUSH map viewer (`pr#33764 <https://github.com/ceph/ceph/pull/33764>`_, Avan Thakkar)
* mgr/dashboard: Add more debug information to Dashboard RGW backend (`pr#34399 <https://github.com/ceph/ceph/pull/34399>`_, Volker Theile)
* mgr/dashboard: Dashboard does not allow you to set norebalance OSD flag (`pr#33927 <https://github.com/ceph/ceph/pull/33927>`_, Nizamudeen)
* mgr/dashboard: Disable cache for static files (`pr#33763 <https://github.com/ceph/ceph/pull/33763>`_, Tiago Melo)
* mgr/dashboard: Display the aggregated number of request (`pr#35212 <https://github.com/ceph/ceph/pull/35212>`_, Tiago Melo)
* mgr/dashboard: Fix HomeTest setup (`pr#35086 <https://github.com/ceph/ceph/pull/35086>`_, Tiago Melo)
* mgr/dashboard: Fix cherrypy request logging error (`pr#31586 <https://github.com/ceph/ceph/pull/31586>`_, Kiefer Chang)
* mgr/dashboard: Fix error in unit test caused by timezone (`pr#34473 <https://github.com/ceph/ceph/pull/34473>`_, Tiago Melo)
* mgr/dashboard: Fix error when listing RBD while deleting or moving (`pr#34120 <https://github.com/ceph/ceph/pull/34120>`_, Tiago Melo)
* mgr/dashboard: Fix iSCSI's username and password validation (`pr#34550 <https://github.com/ceph/ceph/pull/34550>`_, Tiago Melo)
* mgr/dashboard: Fixes rbd image 'purge trash' button & modal text (`pr#33697 <https://github.com/ceph/ceph/pull/33697>`_, anurag)
* mgr/dashboard: Improve workaround to redraw datatables (`pr#34413 <https://github.com/ceph/ceph/pull/34413>`_, Volker Theile)
* mgr/dashboard: Not able to restrict bucket creation for new user (`pr#34692 <https://github.com/ceph/ceph/pull/34692>`_, Volker Theile)
* mgr/dashboard: Pool read/write OPS shows too many decimal places (`pr#34039 <https://github.com/ceph/ceph/pull/34039>`_, anurag, Ernesto Puerta)
* mgr/dashboard: Prevent iSCSI target recreation when editing controls (`pr#34551 <https://github.com/ceph/ceph/pull/34551>`_, Tiago Melo)
* mgr/dashboard: REST API: OpenAPI docs require internet connection (`pr#33032 <https://github.com/ceph/ceph/pull/33032>`_, Patrick Seidensal)
* mgr/dashboard: RGW port autodetection does not support "Beast" RGW frontend (`pr#34400 <https://github.com/ceph/ceph/pull/34400>`_, Volker Theile)
* mgr/dashboard: Refactor Python unittests and controller (`pr#34662 <https://github.com/ceph/ceph/pull/34662>`_, Volker Theile)
* mgr/dashboard: Repair broken grafana panels (`pr#34417 <https://github.com/ceph/ceph/pull/34417>`_, Kristoffer Grönlund)
* mgr/dashboard: Searchable objects for table (`pr#32891 <https://github.com/ceph/ceph/pull/32891>`_, Stephan Müller)
* mgr/dashboard: Tabs does not handle click events (`issue#39326 <http://tracker.ceph.com/issues/39326>`_, `pr#34282 <https://github.com/ceph/ceph/pull/34282>`_, Tiago Melo)
* mgr/dashboard: UI fixes (`pr#34038 <https://github.com/ceph/ceph/pull/34038>`_, Avan Thakkar)
* mgr/dashboard: Updated existing E2E tests to match new format (`pr#33024 <https://github.com/ceph/ceph/pull/33024>`_, Nathan Weinberg)
* mgr/dashboard: Use booleanText pipe (`pr#33234 <https://github.com/ceph/ceph/pull/33234>`_, Alfonso Martínez, Volker Theile)
* mgr/dashboard: Use default language when running "npm run build" (`pr#33668 <https://github.com/ceph/ceph/pull/33668>`_, Tiago Melo)
* mgr/dashboard: do not show RGW API keys if only read-only privileges (`pr#33665 <https://github.com/ceph/ceph/pull/33665>`_, Alfonso Martínez)
* mgr/dashboard: fix COVERAGE_PATH in run-backend-api-tests.sh (`pr#34489 <https://github.com/ceph/ceph/pull/34489>`_, Alfonso Martínez)
* mgr/dashboard: fix backport #33764 (`pr#34640 <https://github.com/ceph/ceph/pull/34640>`_, Ernesto Puerta)
* mgr/dashboard: fix error when enabling SSO with cert. file (`pr#34129 <https://github.com/ceph/ceph/pull/34129>`_, Alfonso Martínez)
* mgr/dashboard: fix py2 strptime ImportError (not thread safe) (`pr#35016 <https://github.com/ceph/ceph/pull/35016>`_, Alfonso Martínez)
* mgr/dashboard: fixing RBD purge error in backend (`pr#34847 <https://github.com/ceph/ceph/pull/34847>`_, Kiefer Chang)
* mgr/dashboard: install teuthology using pip (`pr#35174 <https://github.com/ceph/ceph/pull/35174>`_, Nathan Cutler, Kefu Chai)
* mgr/dashboard: list configured prometheus alerts (`pr#34373 <https://github.com/ceph/ceph/pull/34373>`_, Patrick Seidensal, Tiago Melo)
* mgr/dashboard: monitoring menu entry should indicate firing alerts (`pr#34823 <https://github.com/ceph/ceph/pull/34823>`_, Tiago Melo, Volker Theile)
* mgr/dashboard: remove 'config-opt: read' perm. from system roles (`pr#33739 <https://github.com/ceph/ceph/pull/33739>`_, Alfonso Martínez)
* mgr/dashboard: show checkboxes for booleans (`pr#33388 <https://github.com/ceph/ceph/pull/33388>`_, Tatjana Dehler)
* mgr/dashboard: use FQDN for failover redirection (`pr#34497 <https://github.com/ceph/ceph/pull/34497>`_, Ernesto Puerta)
* mgr/insights: fix prune-health-history (`pr#35214 <https://github.com/ceph/ceph/pull/35214>`_, Sage Weil)
* mgr/pg_autoscaler: fix division by zero (`pr#33420 <https://github.com/ceph/ceph/pull/33420>`_, Sage Weil)
* mgr/pg_autoscaler: treat target ratios as weights (`pr#34087 <https://github.com/ceph/ceph/pull/34087>`_, Josh Durgin)
* mgr/prometheus: ceph_pg\_\* metrics contains last value instead of sum across all reported states (`pr#34162 <https://github.com/ceph/ceph/pull/34162>`_, Jacek Suchenia)
* mgr/run-tox-tests: Fix issue with PYTHONPATH (`pr#33688 <https://github.com/ceph/ceph/pull/33688>`_, Brad Hubbard)
* mgr/telegraf: catch FileNotFoundError exception (`pr#34628 <https://github.com/ceph/ceph/pull/34628>`_, Kefu Chai)
* mgr/telemetry: add 'last_upload' to status (`pr#33409 <https://github.com/ceph/ceph/pull/33409>`_, Yaarit Hatuka)
* mgr/telemetry: catch exception during requests.put (`pr#33141 <https://github.com/ceph/ceph/pull/33141>`_, Sage Weil)
* mgr/telemetry: fix UUID and STR concat (`pr#33666 <https://github.com/ceph/ceph/pull/33666>`_, Yaarit Hatuka)
* mgr/telemetry: fix and document proxy usage (`pr#33649 <https://github.com/ceph/ceph/pull/33649>`_, Lars Marowsky-Bree)
* mgr/volumes: Add interface to get subvolume metadata (`pr#34679 <https://github.com/ceph/ceph/pull/34679>`_, Kotresh HR)
* mgr/volumes: fs subvolume clone cancel (`issue#44208 <http://tracker.ceph.com/issues/44208>`_, `pr#34036 <https://github.com/ceph/ceph/pull/34036>`_, Venky Shankar, Michael Fritch)
* mgr/volumes: minor fixes (`pr#35482 <https://github.com/ceph/ceph/pull/35482>`_, Kotresh HR)
* mgr/volumes: synchronize ownership (for symlinks) and inode timestamps for cloned subvolumes (`issue#24880 <http://tracker.ceph.com/issues/24880>`_, `issue#43965 <http://tracker.ceph.com/issues/43965>`_, `pr#33877 <https://github.com/ceph/ceph/pull/33877>`_, Ramana Raja, Rishabh Dave, huanwen ren, Venky Shankar, Jos Collin)
* mgr: Add get_rates_from_data to mgr_util.py (`pr#33893 <https://github.com/ceph/ceph/pull/33893>`_, Stephan Müller, Ernesto Puerta)
* mgr: Improve internal python to c++ interface (`pr#34356 <https://github.com/ceph/ceph/pull/34356>`_, David Zafman)
* mgr: close restful socket after exec (`pr#35213 <https://github.com/ceph/ceph/pull/35213>`_, liushi)
* mgr: force purge normal ceph entities from service map (`issue#44677 <http://tracker.ceph.com/issues/44677>`_, `pr#34563 <https://github.com/ceph/ceph/pull/34563>`_, Venky Shankar)
* mgr: synchronize ClusterState's health and mon_status (`pr#34326 <https://github.com/ceph/ceph/pull/34326>`_, Radoslaw Zarzynski)
* mgr: update "hostname" when we already have the daemon state from that entity (`pr#33834 <https://github.com/ceph/ceph/pull/33834>`_, Kefu Chai)
* mon/FSCommands: Fix 'add_data_pool' command and 'fs new' command (`pr#34774 <https://github.com/ceph/ceph/pull/34774>`_, Ramana Raja)
* mon/OSDMonitor: Always tune priority cache manager memory on all mons (`pr#34916 <https://github.com/ceph/ceph/pull/34916>`_, Sridhar Seshasayee)
* mon/OSDMonitor: allow trimming maps even if osds are down (`pr#34983 <https://github.com/ceph/ceph/pull/34983>`_, Joao Eduardo Luis)
* mon/PGMap: fix summary display of >32bit pg states (`pr#33275 <https://github.com/ceph/ceph/pull/33275>`_, Sage Weil, Adam C. Emerson)
* mon: Get session_map_lock before remove_session (`pr#34677 <https://github.com/ceph/ceph/pull/34677>`_, Xiaofei Cui)
* mon: calculate min_size on osd pool set size (`pr#34585 <https://github.com/ceph/ceph/pull/34585>`_, Deepika Upadhyay)
* mon: disable min pg per osd warning (`pr#34618 <https://github.com/ceph/ceph/pull/34618>`_, Sage Weil)
* mon: fix/improve mon sync over small keys (`pr#33765 <https://github.com/ceph/ceph/pull/33765>`_, Sage Weil)
* mon: stash newer map on bootstrap when addr doesn't match (`pr#34500 <https://github.com/ceph/ceph/pull/34500>`_, Sage Weil)
* monitoring: Fix "10% OSDs down" alert description (`pr#35211 <https://github.com/ceph/ceph/pull/35211>`_, Benoît Knecht)
* monitoring: Fix pool capacity incorrect (`pr#34450 <https://github.com/ceph/ceph/pull/34450>`_, James Cheng)
* monitoring: alert for pool fill up broken (`pr#35137 <https://github.com/ceph/ceph/pull/35137>`_, Volker Theile)
* monitoring: alert for prediction of disk and pool fill up broken (`pr#34394 <https://github.com/ceph/ceph/pull/34394>`_, Patrick Seidensal)
* monitoring: fix RGW grafana chart 'Average GET/PUT Latencies' (`pr#33860 <https://github.com/ceph/ceph/pull/33860>`_, Alfonso Martínez)
* monitoring: fix decimal precision in Grafana %percentages (`pr#34829 <https://github.com/ceph/ceph/pull/34829>`_, Ernesto Puerta)
* monitoring: root volume full alert fires false positives (`pr#34419 <https://github.com/ceph/ceph/pull/34419>`_, Patrick Seidensal)
* osd/OSD: Log slow ops/types to cluster logs (`pr#33503 <https://github.com/ceph/ceph/pull/33503>`_, Sage Weil, Sridhar Seshasayee)
* osd/OSDMap: Show health warning if a pool is configured with size 1 (`pr#31842 <https://github.com/ceph/ceph/pull/31842>`_, Sridhar Seshasayee)
* osd/PeeringState.h: ignore RemoteBackfillReserved in WaitLocalBackfillReserved (`pr#34512 <https://github.com/ceph/ceph/pull/34512>`_, Neha Ojha)
* osd/PeeringState: do not trim pg log past last_update_ondisk (`pr#34957 <https://github.com/ceph/ceph/pull/34957>`_, Samuel Just, xie xingguo)
* osd/PeeringState: transit async_recovery_targets back into acting before backfilling (`pr#32849 <https://github.com/ceph/ceph/pull/32849>`_, xie xingguo)
* osd: dispatch_context and queue split finish on early bail-out (`pr#35024 <https://github.com/ceph/ceph/pull/35024>`_, Sage Weil)
* osd: fix racy accesses to OSD::osdmap (`pr#33530 <https://github.com/ceph/ceph/pull/33530>`_, Radoslaw Zarzynski)
* pybind/mgr/\*: fix config_notify handling of default values (`pr#34116 <https://github.com/ceph/ceph/pull/34116>`_, Nathan Cutler, Sage Weil)
* pybind/mgr: use six==1.14.0 (`pr#34316 <https://github.com/ceph/ceph/pull/34316>`_, Kefu Chai)
* pybind/rbd: RBD.create() method's 'old_format' parameter now defaults to False (`pr#35183 <https://github.com/ceph/ceph/pull/35183>`_, Jason Dillaman)
* pybind/rbd: ensure image is open before permitting operations (`pr#34424 <https://github.com/ceph/ceph/pull/34424>`_, Mykola Golub)
* pybind/rbd: fix no lockers are obtained, ImageNotFound exception will be output (`pr#34388 <https://github.com/ceph/ceph/pull/34388>`_, zhangdaolong)
* rbd: librbd: copy API should not inherit v1 image format by default (`pr#35182 <https://github.com/ceph/ceph/pull/35182>`_, Jason Dillaman)
* rbd: rbd-mirror: improve detection of blacklisted state (`pr#33533 <https://github.com/ceph/ceph/pull/33533>`_, Mykola Golub)
* rgw/kafka: add kafka endpoint support (`pr#32960 <https://github.com/ceph/ceph/pull/32960>`_, Yuval Lifshitz, Willem Jan Withagen, Kefu Chai)
* rgw/notifications: backporting features and bug fix (`pr#34107 <https://github.com/ceph/ceph/pull/34107>`_, Yuval Lifshitz)
* rgw/notifications: fix topic action fail with "MethodNotAllowed" (`issue#44614 <http://tracker.ceph.com/issues/44614>`_, `pr#33978 <https://github.com/ceph/ceph/pull/33978>`_, Yuval Lifshitz)
* rgw/notifications: version id was not sent in versioned buckets (`pr#35181 <https://github.com/ceph/ceph/pull/35181>`_, Yuval Lifshitz)
* rgw:  when you abort a multipart upload request, the quota may be not updated (`pr#33268 <https://github.com/ceph/ceph/pull/33268>`_, Richard Bai(白学余))
* rgw: Add support bucket policy for subuser (`pr#33714 <https://github.com/ceph/ceph/pull/33714>`_, Seena Fallah)
* rgw: Fix dynamic resharding not working for empty zonegroup in period (`pr#33266 <https://github.com/ceph/ceph/pull/33266>`_, Or Friedmann)
* rgw: Fix upload part copy range able to get almost any string (`pr#33265 <https://github.com/ceph/ceph/pull/33265>`_, Or Friedmann)
* rgw: GET/HEAD and PUT operations on buckets w/lifecycle expiration configured do not return x-amz-expiration header (`pr#32924 <https://github.com/ceph/ceph/pull/32924>`_, Matt Benjamin, Yuval Lifshitz)
* rgw: MultipartObjectProcessor supports stripe size > chunk size (`pr#33271 <https://github.com/ceph/ceph/pull/33271>`_, Casey Bodley)
* rgw: ReplaceKeyPrefixWith and ReplaceKeyWith can not set at the same … (`pr#34599 <https://github.com/ceph/ceph/pull/34599>`_, yuliyang)
* rgw: anonomous swift to obj that dont exist should 401 (`pr#35045 <https://github.com/ceph/ceph/pull/35045>`_, Matthew Oliver)
* rgw: clear ent_list for each loop of bucket list (`issue#44394 <http://tracker.ceph.com/issues/44394>`_, `pr#34099 <https://github.com/ceph/ceph/pull/34099>`_, Yao Zongyou)
* rgw: dmclock: wait until the request is handled (`pr#34954 <https://github.com/ceph/ceph/pull/34954>`_, GaryHyg)
* rgw: find oldest period and update RGWMetadataLogHistory() (`pr#34597 <https://github.com/ceph/ceph/pull/34597>`_, Shilpa Jagannath)
* rgw: fix SignatureDoesNotMatch when use ipv6 address in s3 client (`pr#33267 <https://github.com/ceph/ceph/pull/33267>`_, yuliyang)
* rgw: fix bug with (un)ordered bucket listing and marker w/ namespace (`pr#34609 <https://github.com/ceph/ceph/pull/34609>`_, J. Eric Ivancich)
* rgw: fix lc does not delete objects that do not have exactly the same tags as the rule (`pr#35002 <https://github.com/ceph/ceph/pull/35002>`_, Or Friedmann)
* rgw: fix multipart upload's error response (`pr#35019 <https://github.com/ceph/ceph/pull/35019>`_, GaryHyg)
* rgw: fix rgw crash when duration is invalid in sts request (`pr#33273 <https://github.com/ceph/ceph/pull/33273>`_, yuliyang)
* rgw: fix some list buckets handle leak (`pr#34986 <https://github.com/ceph/ceph/pull/34986>`_, Tianshan Qu)
* rgw: get barbican secret key request maybe return error code (`pr#33965 <https://github.com/ceph/ceph/pull/33965>`_, Richard Bai(白学余))
* rgw: increase log level for same or older period pull msg (`pr#34833 <https://github.com/ceph/ceph/pull/34833>`_, Ali Maredia)
* rgw: make max_connections configurable in beast (`pr#33340 <https://github.com/ceph/ceph/pull/33340>`_, Tiago Pasqualini)
* rgw: making implicit_tenants backwards compatible (`issue#24348 <http://tracker.ceph.com/issues/24348>`_, `pr#33749 <https://github.com/ceph/ceph/pull/33749>`_, Marcus Watts)
* rgw: multisite: enforce spawn window for incremental data sync (`pr#33270 <https://github.com/ceph/ceph/pull/33270>`_, Casey Bodley)
* rgw: radosgw-admin: add support for --bucket-id in bucket stats command (`pr#34815 <https://github.com/ceph/ceph/pull/34815>`_, Vikhyat Umrao)
* rgw: radosgw-admin: fix infinite loops in 'datalog list' (`pr#35001 <https://github.com/ceph/ceph/pull/35001>`_, Casey Bodley)
* rgw: reshard: skip stale bucket id entries from reshard queue (`pr#34735 <https://github.com/ceph/ceph/pull/34735>`_, Abhishek Lekshmanan)
* rgw: set bucket attr twice when delete lifecycle config (`pr#34598 <https://github.com/ceph/ceph/pull/34598>`_, zhang Shaowen)
* rgw: set correct storage class for append (`pr#34064 <https://github.com/ceph/ceph/pull/34064>`_, yuliyang)
* rgw: sts: add all http args to req_info (`pr#33355 <https://github.com/ceph/ceph/pull/33355>`_, yuliyang)
* rgw: tune sharded bucket listing (`pr#33675 <https://github.com/ceph/ceph/pull/33675>`_, J. Eric Ivancich)
* tests: migrate qa/ to python3 (`pr#34171 <https://github.com/ceph/ceph/pull/34171>`_, Kefu Chai, Sage Weil, Casey Bodley, Rishabh Dave, Patrick Donnelly, Kyr Shatskyy, Michael Fritch, Xiubo Li, Ilya Dryomov, Alfonso Martínez, Thomas Bechtold)
* tools/cli: bash_completion: Do not auto complete obsolete and hidden cmds (`pr#35117 <https://github.com/ceph/ceph/pull/35117>`_, Kotresh HR)
* tools/cli: ceph_argparse: increment matchcnt on kwargs (`pr#33160 <https://github.com/ceph/ceph/pull/33160>`_, Matthew Oliver, Shyukri Shyukriev)
* tools/rados: Unmask '-o' to restore original behaviour (`pr#33641 <https://github.com/ceph/ceph/pull/33641>`_, Brad Hubbard)

v14.2.9 Nautilus
================

This is the ninth bugfix release of Nautilus. This release fixes a
couple of security issues in RGW & Messenger V2. We recommend all users
to upgrade to this release.

Notable Changes
---------------

- CVE-2020-1759: Fixed nonce reuse in msgr V2 secure mode
- CVE-2020-1760: Fixed XSS due to RGW GetObject header-splitting

v14.2.8 Nautilus
================

This is the eighth update to the Ceph Nautilus release series. This release
fixes issues across a range of subsystems. We recommend that all users upgrade
to this release.

Notable Changes
---------------

* The default value of ``bluestore_min_alloc_size_ssd`` has been changed to 4K to improve performance across all workloads.

* The following OSD memory config options related to bluestore cache autotuning can now
  be configured during runtime:

    - osd_memory_base (default: 768 MB)
    - osd_memory_cache_min (default: 128 MB)
    - osd_memory_expected_fragmentation (default: 0.15)
    - osd_memory_target (default: 4 GB)

  The above options can be set with::

    ceph config set osd <option> <value>

* The MGR now accepts ``profile rbd`` and ``profile rbd-read-only`` user caps.
  These caps can be used to provide users access to MGR-based RBD functionality
  such as ``rbd perf image iostat`` an ``rbd perf image iotop``.

* The configuration value ``osd_calc_pg_upmaps_max_stddev`` used for upmap
  balancing has been removed. Instead use the mgr balancer config
  ``upmap_max_deviation`` which now is an integer number of PGs of deviation
  from the target PGs per OSD.  This can be set with a command like
  ``ceph config set mgr mgr/balancer/upmap_max_deviation 2``.  The default
  ``upmap_max_deviation`` is 5.  There are situations where crush rules
  would not allow a pool to ever have completely balanced PGs.  For example, if
  crush requires 1 replica on each of 3 racks, but there are fewer OSDs in 1 of
  the racks.  In those cases, the configuration value can be increased.

* RGW: a mismatch between the bucket notification documentation and the actual
  message format was fixed. This means that any endpoints receiving bucket
  notification, will now receive the same notifications inside a JSON array
  named 'Records'. Note that this does not affect pulling bucket notification
  from a subscription in a 'pubsub' zone, as these are already wrapped inside
  that array.

* CephFS: multiple active MDS forward scrub is now rejected. Scrub currently
  only is permitted on a file system with a single rank. Reduce the ranks to one
  via ``ceph fs set <fs_name> max_mds 1``.

* Ceph now refuses to create a file system with a default EC data pool. For
  further explanation, see:
  https://docs.ceph.com/docs/nautilus/cephfs/createfs/#creating-pools

* Ceph will now issue a health warning if a RADOS pool has a ``pg_num``
  value that is not a power of two. This can be fixed by adjusting
  the pool to a nearby power of two::

    ceph osd pool set <pool-name> pg_num <new-pg-num>

  Alternatively, the warning can be silenced with::

    ceph config set global mon_warn_on_pool_pg_num_not_power_of_two false


Changelog
---------

* bluestore: common/options: bluestore 4k min_alloc_size for SSD (`pr#32998 <https://github.com/ceph/ceph/pull/32998>`_, Mark Nelson, Sage Weil)
* bluestore: os/bluestore: Add config observer for osd memory specific options (`pr#31852 <https://github.com/ceph/ceph/pull/31852>`_, Sridhar Seshasayee)
* bluestore: os/bluestore/BlueStore.cc: set priorities for compression stats (`pr#32845 <https://github.com/ceph/ceph/pull/32845>`_, Neha Ojha)
* bluestore: os/bluestore: default bluestore_block_size 1T -> 100G (`pr#32283 <https://github.com/ceph/ceph/pull/32283>`_, Sage Weil)
* build/ops: cmake: remove seastar tests from "make check" (`pr#32658 <https://github.com/ceph/ceph/pull/32658>`_, Kefu Chai)
* build/ops: install-deps,rpm: enable devtoolset-8 on aarch64 also (`issue#38892 <http://tracker.ceph.com/issues/38892>`_, `pr#32651 <https://github.com/ceph/ceph/pull/32651>`_, Kefu Chai)
* build/ops: rpm: add rpm-build to SUSE-specific make check deps (`pr#32208 <https://github.com/ceph/ceph/pull/32208>`_, Nathan Cutler)
* build/ops: switch to boost 1.72 (`pr#32441 <https://github.com/ceph/ceph/pull/32441>`_, Willem Jan Withagen, Kefu Chai)
* build/ops: tools/setup-virtualenv.sh: do not default to python2.7 (`pr#30739 <https://github.com/ceph/ceph/pull/30739>`_, Nathan Cutler)
* cephfs: cephfs-journal-tool: fix crash and usage (`pr#32913 <https://github.com/ceph/ceph/pull/32913>`_, Xiubo Li)
* cephfs: client: Add is_dir() check before changing directory (`pr#32916 <https://github.com/ceph/ceph/pull/32916>`_, Varsha Rao)
* cephfs: client: add procession of SEEK_HOLE and SEEK_DATA in lseek (`pr#30764 <https://github.com/ceph/ceph/pull/30764>`_, Shen Hang)
* cephfs: client: add warning when cap != in->auth_cap (`pr#32065 <https://github.com/ceph/ceph/pull/32065>`_, Shen Hang)
* cephfs: client: EINVAL may be returned when offset is 0 (`pr#30762 <https://github.com/ceph/ceph/pull/30762>`_, wenpengLi)
* cephfs: client: fix lazyio_synchronize() to update file size and libcephfs: Add Tests for LazyIO (`pr#30769 <https://github.com/ceph/ceph/pull/30769>`_, Sidharth Anupkrishnan)
* cephfs: client: _readdir_cache_cb() may use the readdir_cache already clear (`issue#41148 <http://tracker.ceph.com/issues/41148>`_, `pr#30763 <https://github.com/ceph/ceph/pull/30763>`_, huanwen ren)
* cephfs: client: remove Inode.dir_contacts field and handle bad whence value to llseek gracefully (`pr#30766 <https://github.com/ceph/ceph/pull/30766>`_, Jeff Layton)
* cephfs,common: osdc/objecter: Fix last_sent in scientific format and add age to ops (`pr#31081 <https://github.com/ceph/ceph/pull/31081>`_, Varsha Rao)
* cephfs: disallow changing fuse_default_permissions option at runtime (`pr#32915 <https://github.com/ceph/ceph/pull/32915>`_, Zhi Zhang)
* cephfs: mds: add command that config individual client session (`issue#40811 <http://tracker.ceph.com/issues/40811>`_, `pr#32245 <https://github.com/ceph/ceph/pull/32245>`_, "Yan, Zheng")
* cephfs: mds: "apply configuration changes through MDSRank" and "recall caps from quiescent sessions" and "drive cap recall while dropping cache" (`pr#30761 <https://github.com/ceph/ceph/pull/30761>`_, Patrick Donnelly, Jeff Layton)
* cephfs: mds: fix assert(omap_num_objs <= MAX_OBJECTS) of OpenFileTable (`pr#32756 <https://github.com/ceph/ceph/pull/32756>`_, "Yan, Zheng")
* cephfs: mds: fix revoking caps after after stale->resume circle (`pr#32909 <https://github.com/ceph/ceph/pull/32909>`_, "Yan, Zheng")
* cephfs: mds: free heap memory may grow too large for some workloads (`pr#31802 <https://github.com/ceph/ceph/pull/31802>`_, Patrick Donnelly)
* cephfs: MDSMonitor: warn if a new file system is being created with an EC default data pool (`pr#32600 <https://github.com/ceph/ceph/pull/32600>`_, Patrick Donnelly)
* cephfs: mds: no assert on frozen dir when scrub path (`pr#32071 <https://github.com/ceph/ceph/pull/32071>`_, Zhi Zhang)
* cephfs: mds: note client features when rejecting client (`pr#32914 <https://github.com/ceph/ceph/pull/32914>`_, Patrick Donnelly)
* cephfs:  mds/OpenFileTable: match MAX_ITEMS_PER_OBJ to osd_deep_scrub_large_omap_object_key_threshold (`pr#32921 <https://github.com/ceph/ceph/pull/32921>`_, Vikhyat Umrao, Varsha Rao)
* cephfs: mds: properly evaluate unstable locks when evicting client (`pr#32073 <https://github.com/ceph/ceph/pull/32073>`_, "Yan, Zheng")
* cephfs: mds: reject forward scrubs when cluster has multiple active MDS (more than one rank) (`pr#32602 <https://github.com/ceph/ceph/pull/32602>`_, Patrick Donnelly, Milind Changire)
* cephfs: mds: reject sessionless messages (`issue#40784 <http://tracker.ceph.com/issues/40784>`_, `pr#30843 <https://github.com/ceph/ceph/pull/30843>`_, "Yan, Zheng", Xiao Guodong, Shen Hang)
* cephfs: mds: remove unnecessary debug warning (`pr#32077 <https://github.com/ceph/ceph/pull/32077>`_, Patrick Donnelly)
* cephfs: mds returns -5(EIO) error when the deleted file does not exist (`pr#30767 <https://github.com/ceph/ceph/pull/30767>`_, huanwen ren)
* cephfs: mds: split the dir if the op makes it oversized, because some ops maybe in flight (`pr#31302 <https://github.com/ceph/ceph/pull/31302>`_, simon gao)
* cephfs: mds: tolerate no snaprealm encoded in on-disk root inode (`pr#32079 <https://github.com/ceph/ceph/pull/32079>`_, "Yan, Zheng")
* cephfs: mgr: "mds metadata" to setup new DaemonState races with fsmap (`pr#31905 <https://github.com/ceph/ceph/pull/31905>`_, Patrick Donnelly)
* cephfs: mgr/volumes: allow setting uid, gid of subvolume and subvolume group during creation (`issue#42923 <http://tracker.ceph.com/issues/42923>`_, `pr#31741 <https://github.com/ceph/ceph/pull/31741>`_, Venky Shankar, Jos Collin)
* cephfs:  mgr/volumes: fetch trash and clone entries without blocking volume access (`issue#44282 <http://tracker.ceph.com/issues/44282>`_, `pr#33526 <https://github.com/ceph/ceph/pull/33526>`_, Venky Shankar)
* cephfs:  mgr/volumes: fs subvolume resize command (`pr#31332 <https://github.com/ceph/ceph/pull/31332>`_, Jos Collin)
* cephfs: mgr/volumes: misc fix and feature enhancements (`issue#42646 <http://tracker.ceph.com/issues/42646>`_, `issue#43645 <http://tracker.ceph.com/issues/43645>`_, `pr#33122 <https://github.com/ceph/ceph/pull/33122>`_, Rishabh Dave, Joshua Schmid, Venky Shankar, Ramana Raja, Jos Collin)
* cephfs:  mgr/volumes: unregister job upon async threads exception (`issue#44315 <http://tracker.ceph.com/issues/44315>`_, `pr#33569 <https://github.com/ceph/ceph/pull/33569>`_, Venky Shankar)
* cephfs:  mon: print FSMap regardless of file system count (`pr#32912 <https://github.com/ceph/ceph/pull/32912>`_, Patrick Donnelly)
* cephfs:  pybind/mgr/volumes: idle connection drop is not working (`pr#33116 <https://github.com/ceph/ceph/pull/33116>`_, Patrick Donnelly)
* cephfs: RuntimeError: Files in flight high water is unexpectedly low (0 / 6) (`pr#33115 <https://github.com/ceph/ceph/pull/33115>`_, Patrick Donnelly)
* ceph.in: check ceph-conf returncode (`pr#31367 <https://github.com/ceph/ceph/pull/31367>`_, Dimitri Savineau)
* ceph-monstore-tool: correct the key for storing mgr_command_descs (`pr#33278 <https://github.com/ceph/ceph/pull/33278>`_, Kefu Chai)
* ceph-volume: add db and wal support to raw mode (`pr#32979 <https://github.com/ceph/ceph/pull/32979>`_, Sébastien Han)
* ceph-volume: add methods to pass filters to pvs, vgs and lvs commands (`pr#33217 <https://github.com/ceph/ceph/pull/33217>`_, Rishabh Dave)
* ceph-volume: add raw (--bluestore) mode (`pr#32733 <https://github.com/ceph/ceph/pull/32733>`_, Jan Fajerski, Sage Weil)
* ceph-volume: add sizing arguments to prepare (`pr#33231 <https://github.com/ceph/ceph/pull/33231>`_, Jan Fajerski)
* ceph-volume: allow raw block devices everywhere (`pr#32868 <https://github.com/ceph/ceph/pull/32868>`_, Jan Fajerski)
* ceph-volume: assume msgrV1 for all branches containing mimic (`pr#31616 <https://github.com/ceph/ceph/pull/31616>`_, Jan Fajerski)
* ceph-volume: avoid calling zap_lv with a LV-less VG (`pr#33297 <https://github.com/ceph/ceph/pull/33297>`_, Jan Fajerski)
* ceph-volume: batch bluestore fix create_lvs call (`pr#33232 <https://github.com/ceph/ceph/pull/33232>`_, Jan Fajerski)
* ceph-volume: batch bluestore fix create_lvs call (`pr#33301 <https://github.com/ceph/ceph/pull/33301>`_, Jan Fajerski)
* ceph-volume/batch: fail on filtered devices when non-interactive (`pr#33202 <https://github.com/ceph/ceph/pull/33202>`_, Jan Fajerski)
* ceph-volume: Dereference symlink in lvm list (`pr#32877 <https://github.com/ceph/ceph/pull/32877>`_, Benoît Knecht)
* ceph-volume: don't remove vg twice when zapping filestore (`pr#33337 <https://github.com/ceph/ceph/pull/33337>`_, Jan Fajerski)
* ceph-volume: finer grained availability notion in inventory (`pr#33240 <https://github.com/ceph/ceph/pull/33240>`_, Jan Fajerski)
* ceph-volume: fix has_bluestore_label() function (`pr#33239 <https://github.com/ceph/ceph/pull/33239>`_, Guillaume Abrioux)
* ceph-volume: fix is_ceph_device for lvm batch (`pr#33253 <https://github.com/ceph/ceph/pull/33253>`_, Jan Fajerski, Dimitri Savineau)
* ceph-volume: fix the integer overflow (`pr#32873 <https://github.com/ceph/ceph/pull/32873>`_, dongdong tao)
* ceph-volume: import mock.mock instead of unittest.mock (py2) (`pr#32870 <https://github.com/ceph/ceph/pull/32870>`_, Jan Fajerski)
* ceph-volume/lvm/activate.py: clarify error message: fsid refers to osd_fsid (`pr#32864 <https://github.com/ceph/ceph/pull/32864>`_, Yaniv Kaul)
* ceph-volume: lvm/deactivate: add unit tests, remove --all (`pr#32863 <https://github.com/ceph/ceph/pull/32863>`_, Jan Fajerski)
* ceph-volume: lvm deactivate command (`pr#33209 <https://github.com/ceph/ceph/pull/33209>`_, Jan Fajerski)
* ceph-volume: make get_devices fs location independent (`pr#33200 <https://github.com/ceph/ceph/pull/33200>`_, Jan Fajerski)
* ceph-volume: minor clean-up of "simple scan" subcommand help (`pr#32556 <https://github.com/ceph/ceph/pull/32556>`_, Michael Fritch)
* ceph-volume: pass journal_size as Size not string (`pr#33334 <https://github.com/ceph/ceph/pull/33334>`_, Jan Fajerski)
* ceph-volume: refactor listing.py + fixes (`pr#33238 <https://github.com/ceph/ceph/pull/33238>`_, Jan Fajerski, Rishabh Dave, Guillaume Abrioux)
* ceph-volume: reject disks smaller then 5GB in inventory (`issue#40776 <http://tracker.ceph.com/issues/40776>`_, `pr#31554 <https://github.com/ceph/ceph/pull/31554>`_, Jan Fajerski)
* ceph-volume: skip osd creation when already done (`pr#33242 <https://github.com/ceph/ceph/pull/33242>`_, Guillaume Abrioux)
* ceph-volume/test: patch VolumeGroups (`pr#32558 <https://github.com/ceph/ceph/pull/32558>`_, Jan Fajerski)
* ceph-volume: use correct extents if using db-devices and >1 osds_per_device (`pr#32874 <https://github.com/ceph/ceph/pull/32874>`_, Fabian Niepelt)
* ceph-volume: use fsync for dd command (`pr#31553 <https://github.com/ceph/ceph/pull/31553>`_, Rishabh Dave)
* ceph-volume: use get_device_vgs in has_common_vg (`pr#33254 <https://github.com/ceph/ceph/pull/33254>`_, Jan Fajerski)
* ceph-volume: util: look for executable in $PATH (`pr#32860 <https://github.com/ceph/ceph/pull/32860>`_, Shyukri Shyukriev)
* ceph-volume/zfs: add the inventory command (`pr#31295 <https://github.com/ceph/ceph/pull/31295>`_, Willem Jan Withagen)
* common/admin_socket: Increase socket timeouts (`pr#32063 <https://github.com/ceph/ceph/pull/32063>`_, Brad Hubbard)
* common/bl: fix the dangling last_p issue (`pr#33277 <https://github.com/ceph/ceph/pull/33277>`_, Radoslaw Zarzynski)
* common/config: update values when they are removed via mon (`pr#32846 <https://github.com/ceph/ceph/pull/32846>`_, Sage Weil)
* common: FIPS: audit and switch some memset & bzero users (`pr#32167 <https://github.com/ceph/ceph/pull/32167>`_, Radoslaw Zarzynski)
* common: fix deadlocky inflight op visiting in OpTracker (`pr#32858 <https://github.com/ceph/ceph/pull/32858>`_, Radoslaw Zarzynski)
* common/options: remove unused ms_msgr2\_{sign,encrypt} (`pr#31850 <https://github.com/ceph/ceph/pull/31850>`_, Ilya Dryomov)
* common/util: use ifstream to read from /proc files (`pr#32901 <https://github.com/ceph/ceph/pull/32901>`_, Kefu Chai, songweibin)
* core: auth/Crypto: fallback to /dev/urandom if getentropy() fails (`pr#31301 <https://github.com/ceph/ceph/pull/31301>`_, Kefu Chai)
* core: mon: keep v1 address type when explicitly set (`pr#32028 <https://github.com/ceph/ceph/pull/32028>`_, Ricardo Dias)
* core: mon/OSDMonitor: Fix pool set target_size_bytes (etc) with unit suffix (`pr#31740 <https://github.com/ceph/ceph/pull/31740>`_, Prashant D)
* core: osd/OSDMap: health alert for non-power-of-two pg_num (`pr#30689 <https://github.com/ceph/ceph/pull/30689>`_, Sage Weil)
* crush/CrushWrapper: behave with empty weight vector (`pr#32905 <https://github.com/ceph/ceph/pull/32905>`_, Kefu Chai)
* doc/cephfs/client-auth: description and example are inconsistent (`pr#32781 <https://github.com/ceph/ceph/pull/32781>`_, Ilya Dryomov)
* doc/cephfs: improve add/remove MDS section (`issue#39620 <http://tracker.ceph.com/issues/39620>`_, `pr#31116 <https://github.com/ceph/ceph/pull/31116>`_, Patrick Donnelly)
* doc/ceph-fuse: mention -k option in ceph-fuse man page (`pr#30765 <https://github.com/ceph/ceph/pull/30765>`_, Rishabh Dave)
* doc/ceph-volume: initial docs for zfs/inventory and zfs/api (`pr#32746 <https://github.com/ceph/ceph/pull/32746>`_, Willem Jan Withagen)
* doc: remove invalid option mon_pg_warn_max_per_osd (`pr#31300 <https://github.com/ceph/ceph/pull/31300>`_, zhang daolong)
* doc/_templates/page.html: redirect to etherpad (`pr#32248 <https://github.com/ceph/ceph/pull/32248>`_, Neha Ojha)
* doc: wrong datatype describing crush_rule (`pr#32254 <https://github.com/ceph/ceph/pull/32254>`_, Kefu Chai)
* global: disable THP for Ceph daemons (`pr#31646 <https://github.com/ceph/ceph/pull/31646>`_, Patrick Donnelly, Mark Nelson)
* kv: fix shutdown vs async compaction (`pr#32715 <https://github.com/ceph/ceph/pull/32715>`_, Sage Weil)
* librbd: diff iterate with fast-diff now correctly includes parent (`pr#32469 <https://github.com/ceph/ceph/pull/32469>`_, Jason Dillaman)
* librbd: fix rbd_open_by_id, rbd_open_by_id_read_only (`pr#32837 <https://github.com/ceph/ceph/pull/32837>`_, yangjun)
* librbd: remove pool objects when removing a namespace (`pr#32839 <https://github.com/ceph/ceph/pull/32839>`_, Jason Dillaman)
* librbd: skip stale child with non-existent pool for list descendants (`pr#32841 <https://github.com/ceph/ceph/pull/32841>`_, songweibin)
* librbd: support compression allocation hints to the OSD (`pr#32842 <https://github.com/ceph/ceph/pull/32842>`_, Jason Dillaman)
* mgr: add 'rbd' profiles to support 'rbd_support' module commands (`pr#32086 <https://github.com/ceph/ceph/pull/32086>`_, Jason Dillaman)
* mgr/alerts: simple health alerts (`pr#30820 <https://github.com/ceph/ceph/pull/30820>`_, Sage Weil)
* mgr: Balancer fixes (`pr#31956 <https://github.com/ceph/ceph/pull/31956>`_, Neha Ojha, Kefu Chai, David Zafman)
* mgr/DaemonServer: fix 'osd ok-to-stop' for EC pools (`pr#32844 <https://github.com/ceph/ceph/pull/32844>`_, Sage Weil)
* mgr/dashboard: add debug mode, and accept expected exception when SSL handshaking (`pr#31190 <https://github.com/ceph/ceph/pull/31190>`_, Kefu Chai, Ernesto Puerta, Joshua Schmid)
* mgr/dashboard: block mirroring page results in internal server error (`pr#32133 <https://github.com/ceph/ceph/pull/32133>`_, Jason Dillaman)
* mgr/dashboard: check embedded Grafana dashboard references (`issue#40008 <http://tracker.ceph.com/issues/40008>`_, `pr#31808 <https://github.com/ceph/ceph/pull/31808>`_, Kiefer Chang)
* mgr/dashboard: check if user has config-opt permissions (`pr#32827 <https://github.com/ceph/ceph/pull/32827>`_, Alfonso Martínez)
* mgr/dashboard: Cross sign button not working for some modals (`pr#32012 <https://github.com/ceph/ceph/pull/32012>`_, Ricardo Marques)
* mgr/dashboard: Dashboard can't handle self-signed cert on Grafana API (`pr#31792 <https://github.com/ceph/ceph/pull/31792>`_, Volker Theile)
* mgr/dashboard: disable 'Add Capability' button in rgw user edit (`pr#32930 <https://github.com/ceph/ceph/pull/32930>`_, Alfonso Martínez)
* mgr/dashboard: fix restored RBD image naming issue (`pr#31810 <https://github.com/ceph/ceph/pull/31810>`_, Kiefer Chang)
* mgr/dashboard: grafana charts match time picker selection (`pr#31999 <https://github.com/ceph/ceph/pull/31999>`_, Alfonso Martínez)
* mgr/dashboard,grafana: remove shortcut menu (`pr#31980 <https://github.com/ceph/ceph/pull/31980>`_, Ernesto Puerta)
* mgr/dashboard: Handle always-on Ceph Manager modules correctly (`pr#31782 <https://github.com/ceph/ceph/pull/31782>`_, Volker Theile)
* mgr/dashboard: Hardening accessing the metadata (`pr#32128 <https://github.com/ceph/ceph/pull/32128>`_, Volker Theile)
* mgr/dashboard: iSCSI targets not available if any gateway is down (and more...) (`pr#32304 <https://github.com/ceph/ceph/pull/32304>`_, Ricardo Marques)
* mgr/dashboard: KeyError on dashboard reload (`pr#32233 <https://github.com/ceph/ceph/pull/32233>`_, Patrick Seidensal)
* mgr/dashboard: key-value-table doesn't render booleans (`pr#31789 <https://github.com/ceph/ceph/pull/31789>`_, Patrick Seidensal)
* mgr/dashboard: Remove compression mode unset in pool from (`pr#31784 <https://github.com/ceph/ceph/pull/31784>`_, Stephan Müller)
* mgr/dashboard: show "Rename" in header & button when renaming RBD (`pr#31779 <https://github.com/ceph/ceph/pull/31779>`_, Alfonso Martínez)
* mgr/dashboard: sort monitors by open sessions correctly (`pr#31791 <https://github.com/ceph/ceph/pull/31791>`_, Alfonso Martínez)
* mgr/dashboard: Standby Dashboards don't handle all requests properly (`pr#32299 <https://github.com/ceph/ceph/pull/32299>`_, Volker Theile)
* mgr/dashboard: Trim IQN on iSCSI target form (`pr#31942 <https://github.com/ceph/ceph/pull/31942>`_, Ricardo Marques)
* mgr/dashboard: Unable to set boolean values to false when default is true (`pr#31941 <https://github.com/ceph/ceph/pull/31941>`_, Ricardo Marques)
* mgr/dashboard: Using wrong identifiers in RGW user/bucket datatables (`pr#32888 <https://github.com/ceph/ceph/pull/32888>`_, Volker Theile)
* mgr/devicehealth: ensure we don't store empty objects (`pr#31735 <https://github.com/ceph/ceph/pull/31735>`_, Sage Weil)
* mgr/devicehealth: fix telemetry stops sending device reports after 48 hours (`pr#33346 <https://github.com/ceph/ceph/pull/33346>`_, Yaarit Hatuka, Sage Weil)
* mgr: drop reference to msg on return (`pr#33498 <https://github.com/ceph/ceph/pull/33498>`_, Patrick Donnelly)
* mgr/MgrClient: fix open condition (`pr#32769 <https://github.com/ceph/ceph/pull/32769>`_, Sage Weil)
* mgr/pg_autoscaler: calculate pool_pg_target using pool size (`pr#33170 <https://github.com/ceph/ceph/pull/33170>`_, Dan van der Ster)
* mgr/pg_autoscaler: default to pg_num[_min] = 16 (`pr#32069 <https://github.com/ceph/ceph/pull/32069>`_, Sage Weil)
* mgr/pg_autoscaler: default to pg_num[_min] = 32 (`pr#32931 <https://github.com/ceph/ceph/pull/32931>`_, Neha Ojha)
* mgr/pg_autoscaler: implement shutdown method (`pr#32068 <https://github.com/ceph/ceph/pull/32068>`_, Patrick Donnelly)
* mgr/pg_autoscaler: only generate target\_\* health warnings if targets set (`pr#32067 <https://github.com/ceph/ceph/pull/32067>`_, Sage Weil)
* mgr/prometheus: assign a value to osd_dev_node when obj_store is not filestore or bluestore (`pr#31556 <https://github.com/ceph/ceph/pull/31556>`_, jiahuizeng)
* mgr/prometheus: report per-pool pg states (`pr#33157 <https://github.com/ceph/ceph/pull/33157>`_, Aleksei Zakharov)
* mgr/telemetry: anonymizing smartctl report itself (`pr#33082 <https://github.com/ceph/ceph/pull/33082>`_, Yaarit Hatuka)
* mgr/telemetry: check get_metadata return val (`pr#33095 <https://github.com/ceph/ceph/pull/33095>`_, Yaarit Hatuka)
* mgr/telemetry: split entity_name only once (handle ids with dots) (`pr#33168 <https://github.com/ceph/ceph/pull/33168>`_, Dan Mick)
* mgr/zabbix: Adds possibility to send data to multiple zabbix servers (`pr#30009 <https://github.com/ceph/ceph/pull/30009>`_, slivik, Jakub Sliva)
* mon/ConfigMonitor: fix handling of NO_MON_UPDATE settings (`pr#32856 <https://github.com/ceph/ceph/pull/32856>`_, Sage Weil)
* mon/ConfigMonitor: only propose if leader (`pr#33155 <https://github.com/ceph/ceph/pull/33155>`_, Sage Weil)
* mon: Don't put session during feature change (`pr#33152 <https://github.com/ceph/ceph/pull/33152>`_, Brad Hubbard)
* mon: elector: return after triggering a new election (`pr#33007 <https://github.com/ceph/ceph/pull/33007>`_, Greg Farnum)
* monitoring: wait before firing osd full alert (`pr#32070 <https://github.com/ceph/ceph/pull/32070>`_, Patrick Seidensal)
* mon/MgrMonitor.cc: add always_on_modules to the output of "ceph mgr module ls" (`pr#32997 <https://github.com/ceph/ceph/pull/32997>`_, Neha Ojha)
* mon/MgrMonitor.cc: warn about missing mgr in a cluster with osds (`pr#33142 <https://github.com/ceph/ceph/pull/33142>`_, Neha Ojha)
* mon/OSDMonitor: Don't update mon cache settings if rocksdb is not used (`pr#32520 <https://github.com/ceph/ceph/pull/32520>`_, Sridhar Seshasayee, Sage Weil)
* mon/OSDMonitor: fix format error ceph osd stat --format json (`pr#32062 <https://github.com/ceph/ceph/pull/32062>`_, Zheng Yin)
* mon/PGMap.h: disable network stats in dump_osd_stats (`pr#32466 <https://github.com/ceph/ceph/pull/32466>`_, Neha Ojha, David Zafman)
* mon: remove the restriction of address type in init_with_hosts (`pr#31844 <https://github.com/ceph/ceph/pull/31844>`_, Hao Xiong)
* mon/Session: only index osd ids >= 0 (`pr#32908 <https://github.com/ceph/ceph/pull/32908>`_, Sage Weil)
* mount.ceph: give a hint message when no mds is up or cluster is laggy (`pr#32910 <https://github.com/ceph/ceph/pull/32910>`_, Xiubo Li)
* mount.ceph: remove arbitrary limit on size of name= option (`pr#32807 <https://github.com/ceph/ceph/pull/32807>`_, Jeff Layton)
* msg: async/net_handler.cc: Fix compilation (`pr#31736 <https://github.com/ceph/ceph/pull/31736>`_, Carlos Valiente)
* osd: add osd_fast_shutdown option (default true) (`pr#32743 <https://github.com/ceph/ceph/pull/32743>`_, Sage Weil)
* osd: Allow 64-char hostname to be added as the "host" in CRUSH (`pr#33147 <https://github.com/ceph/ceph/pull/33147>`_, Michal Skalski)
* osd: Diagnostic logging for upmap cleaning (`pr#32716 <https://github.com/ceph/ceph/pull/32716>`_, David Zafman)
* osd/OSD: enhance osd numa affinity compatibility (`pr#32843 <https://github.com/ceph/ceph/pull/32843>`_, luo rixin, Dai zhiwei)
* osd/PeeringState.cc: don't let num_objects become negative (`pr#32857 <https://github.com/ceph/ceph/pull/32857>`_, Neha Ojha)
* osd/PeeringState.cc: skip peer_purged when discovering all missing (`pr#32847 <https://github.com/ceph/ceph/pull/32847>`_, Neha Ojha)
* osd/PeeringState: do not exclude up from acting_recovery_backfill (`pr#32064 <https://github.com/ceph/ceph/pull/32064>`_, Nathan Cutler, xie xingguo)
* osd/PrimaryLogPG: skip obcs that don't exist during backfill scan_range (`pr#31028 <https://github.com/ceph/ceph/pull/31028>`_, Sage Weil)
* osd: set affinity for \*all\* threads (`pr#31359 <https://github.com/ceph/ceph/pull/31359>`_, Sage Weil)
* osd: set collection pool opts on collection create, pg load (`pr#32123 <https://github.com/ceph/ceph/pull/32123>`_, Sage Weil)
* osd: Use physical ratio for nearfull (doesn't include backfill resserve) (`pr#32773 <https://github.com/ceph/ceph/pull/32773>`_, David Zafman)
* pybind/mgr: Cancel output color control (`pr#31697 <https://github.com/ceph/ceph/pull/31697>`_, Zheng Yin)
* rbd:  creating thick-provision image progress percent info exceeds 100% (`pr#32840 <https://github.com/ceph/ceph/pull/32840>`_, Xiangdong Mu)
* rbd: librbd: don't call refresh from mirror::GetInfoRequest state machine (`pr#32900 <https://github.com/ceph/ceph/pull/32900>`_, Mykola Golub)
* rbd-mirror: clone v2 mirroring improvements (`pr#31518 <https://github.com/ceph/ceph/pull/31518>`_, Mykola Golub)
* rbd-mirror: fix 'rbd mirror status' asok command output (`pr#32447 <https://github.com/ceph/ceph/pull/32447>`_, Mykola Golub)
* rbd-mirror: make logrotate work (`pr#32593 <https://github.com/ceph/ceph/pull/32593>`_, Mykola Golub)
* rgw: add bucket permission verify when copy obj (`pr#31089 <https://github.com/ceph/ceph/pull/31089>`_, NancySu05)
* rgw: Adding 'iam' namespace for Role and User Policy related REST APIs (`pr#32437 <https://github.com/ceph/ceph/pull/32437>`_, Pritha Srivastava)
* rgw: adding mfa code validation when bucket versioning status is changed (`pr#32759 <https://github.com/ceph/ceph/pull/32759>`_, Pritha Srivastava)
* rgw: add num_shards to radosgw-admin bucket stats (`pr#31182 <https://github.com/ceph/ceph/pull/31182>`_, Paul Emmerich)
* rgw: allow reshard log entries for non-existent buckets to be cancelled (`pr#32056 <https://github.com/ceph/ceph/pull/32056>`_, J. Eric Ivancich)
* rgw: auto-clean reshard queue entries for non-existent buckets (`pr#32055 <https://github.com/ceph/ceph/pull/32055>`_, J. Eric Ivancich)
* rgw: build_linked_oids_for_bucket and build_buckets_instance_index should return negative value if it fails (`pr#32820 <https://github.com/ceph/ceph/pull/32820>`_, zhangshaowen)
* rgw: crypt: permit RGW-AUTO/default with SSE-S3 headers (`pr#31862 <https://github.com/ceph/ceph/pull/31862>`_, Matt Benjamin)
* rgw: data sync markers include timestamp from datalog entry (`pr#32819 <https://github.com/ceph/ceph/pull/32819>`_, Casey Bodley)
* rgw_file: avoid string::front() on empty path (`pr#33008 <https://github.com/ceph/ceph/pull/33008>`_, Matt Benjamin)
* rgw: fix a bug that bucket instance obj can't be removed after resharding completed (`pr#32822 <https://github.com/ceph/ceph/pull/32822>`_, zhang Shaowen)
* rgw: fix an endless loop error when to show usage (`pr#31684 <https://github.com/ceph/ceph/pull/31684>`_, lvshuhua)
* rgw: fix bugs in listobjectsv1 (`pr#32239 <https://github.com/ceph/ceph/pull/32239>`_, Albin Antony)
* rgw: fix compile errors with boost 1.70 (`pr#31289 <https://github.com/ceph/ceph/pull/31289>`_, Casey Bodley)
* rgw: fix data consistency error casued by rgw sent timeout (`pr#32821 <https://github.com/ceph/ceph/pull/32821>`_, 李纲彬82225)
* rgw: fix list versions starts with version_id=null (`pr#30743 <https://github.com/ceph/ceph/pull/30743>`_, Tianshan Qu)
* rgw: fix one part of the bulk delete(RGWDeleteMultiObj_ObjStore_S3) fails but no error messages (`pr#33151 <https://github.com/ceph/ceph/pull/33151>`_, Snow Si)
* rgw: fix opslog operation field as per Amazon s3 (`issue#20978 <http://tracker.ceph.com/issues/20978>`_, `pr#32834 <https://github.com/ceph/ceph/pull/32834>`_, Jiaying Ren)
* rgw: fix refcount tags to match and update object's idtag (`pr#30741 <https://github.com/ceph/ceph/pull/30741>`_, J. Eric Ivancich)
* rgw: fix rgw crash when token is not base64 encode (`pr#32050 <https://github.com/ceph/ceph/pull/32050>`_, yuliyang)
* rgw: gc remove tag after all sub io finish (`issue#40903 <http://tracker.ceph.com/issues/40903>`_, `pr#30733 <https://github.com/ceph/ceph/pull/30733>`_, Tianshan Qu)
* rgw: Incorrectly calling ceph::buffer::list::decode_base64 in bucket policy (`pr#32832 <https://github.com/ceph/ceph/pull/32832>`_, GaryHyg)
* rgw: maybe coredump when reload operator happened (`pr#33149 <https://github.com/ceph/ceph/pull/33149>`_, Richard Bai(白学余))
* rgw: move forward marker even in case of many rgw.none indexes (`pr#32824 <https://github.com/ceph/ceph/pull/32824>`_, Ilsoo Byun)
* rgw multisite: fixes for concurrent version creation (`pr#32057 <https://github.com/ceph/ceph/pull/32057>`_, Or Friedmann, Casey Bodley)
* rgw: prevent bucket reshard scheduling if bucket is resharding (`pr#31298 <https://github.com/ceph/ceph/pull/31298>`_, J. Eric Ivancich)
* rgw/pubsub: fix records/event json format to match documentation (`pr#32221 <https://github.com/ceph/ceph/pull/32221>`_, Yuval Lifshitz)
* rgw: radosgw-admin: sync status displays id of shard furthest behind (`pr#32818 <https://github.com/ceph/ceph/pull/32818>`_, Casey Bodley)
* rgw: return error if lock log shard fails (`pr#32825 <https://github.com/ceph/ceph/pull/32825>`_, zhangshaowen)
* rgw/rgw_rest_conn.h: fix build with clang (`pr#32489 <https://github.com/ceph/ceph/pull/32489>`_, Bernd Zeimetz)
* rgw: Select the std::bitset to resolv ambiguity (`pr#32504 <https://github.com/ceph/ceph/pull/32504>`_, Willem Jan Withagen)
* rgw: support radosgw-admin zone/zonegroup placement get command (`pr#32835 <https://github.com/ceph/ceph/pull/32835>`_, jiahuizeng)
* rgw: the http response code of delete bucket should not be 204-no-content (`pr#32833 <https://github.com/ceph/ceph/pull/32833>`_, Chang Liu)
* rgw:  update s3-test download code for s3-test tasks (`pr#32229 <https://github.com/ceph/ceph/pull/32229>`_, Ali Maredia)
* rgw: update the hash source for multipart entries during resharding (`pr#33183 <https://github.com/ceph/ceph/pull/33183>`_, dongdong tao)
* rgw: url encode common prefixes for List Objects response (`pr#32058 <https://github.com/ceph/ceph/pull/32058>`_, Abhishek Lekshmanan)
* rgw: when resharding store progress json (`pr#31683 <https://github.com/ceph/ceph/pull/31683>`_, Mark Kogan, Mark Nelson)
* selinux: Allow ceph to read udev db (`pr#32259 <https://github.com/ceph/ceph/pull/32259>`_, Boris Ranto)


v14.2.7 Nautilus
================

This is the seventh update to the Ceph Nautilus release series. This is
a hotfix release primarily fixing a couple of security issues. We
recommend that all users upgrade to this release.

Notable Changes
---------------

* CVE-2020-1699: Fixed a path traversal flaw in Ceph dashboard that
  could allow for potential information disclosure (Ernesto Puerta)
* CVE-2020-1700: Fixed a flaw in RGW beast frontend that could lead to
  denial of service from an unauthenticated client (Or Friedmann)


v14.2.6 Nautilus
================

This is the sixth update to the Ceph Nautilus release series. This is a hotfix
release primarily fixing a regression introduced in v14.2.5, all nautilus users
are advised to upgrade to this release.

Notable Changes
---------------

* This release fixes a ``ceph-mgr`` bug that caused mgr becoming unresponsive on
  larger clusters `issue#43364 <https://tracker.ceph.com/issues/43364>`_ (`pr#32466 <https://github.com/ceph/ceph/pull/32466>`_, David Zafman, Neha Ojha)


v14.2.5 Nautilus
================

This is the fifth release of the Ceph Nautilus release series. Among the many
notable changes, this release fixes a critical BlueStore bug that was introduced
in 14.2.3. All Nautilus users are advised to upgrade to this release.

Notable Changes
---------------

Critical fix:

* This release fixes a `critical BlueStore bug <https://tracker.ceph.com/issues/42223>`_
  introduced in 14.2.3 (and also present in 14.2.4) that can lead to data
  corruption when a separate "WAL" device is used.

New health warnings:

* Ceph will now issue health warnings if daemons have recently crashed. Ceph
  has been collecting crash reports since the initial Nautilus release, but the
  health alerts are new. To view new crashes (or all crashes, if you've just
  upgraded)::

    ceph crash ls-new

  To acknowledge a particular crash (or all crashes) and silence the health warning::

    ceph crash archive <crash-id>
    ceph crash archive-all

* Ceph will issue a health warning if a RADOS pool's ``size`` is set to 1
  or, in other words, if the pool is configured with no redundancy. Ceph will
  stop issuing the warning if the pool size is set to the minimum
  recommended value::

    ceph osd pool set <pool-name> size <num-replicas>

  The warning can be silenced with::

    ceph config set global mon_warn_on_pool_no_redundancy false

* A health warning is now generated if the average osd heartbeat ping
  time exceeds a configurable threshold for any of the intervals
  computed. The OSD computes 1 minute, 5 minute and 15 minute
  intervals with average, minimum and maximum values.  New configuration
  option `mon_warn_on_slow_ping_ratio` specifies a percentage of
  `osd_heartbeat_grace` to determine the threshold.  A value of zero
  disables the warning. New configuration option `mon_warn_on_slow_ping_time`
  specified in milliseconds over-rides the computed value, causes a warning
  when OSD heartbeat pings take longer than the specified amount.
  A new admin command, `ceph daemon mgr.# dump_osd_network [threshold]`, will
  list all connections with a ping time longer than the specified threshold or
  value determined by the config options, for the average for any of the 3 intervals.
  Another new admin command, `ceph daemon osd.# dump_osd_network [threshold]`,
  will do the same but only including heartbeats initiated by the specified OSD.

Changes in the telemetry module:

* The telemetry module now reports more information.

  First, there is a new 'device' channel, enabled by default, that
  will report anonymized hard disk and SSD health metrics to
  telemetry.ceph.com in order to build and improve device failure
  prediction algorithms.  If you are not comfortable sharing device
  metrics, you can disable that channel first before re-opting-in::

    ceph config set mgr mgr/telemetry/channel_device false

  Second, we now report more information about CephFS file systems,
  including:

    - how many MDS daemons (in total and per file system)
    - which features are (or have been) enabled
    - how many data pools
    - approximate file system age (year + month of creation)
    - how many files, bytes, and snapshots
    - how much metadata is being cached

  We have also added:

    - which Ceph release the monitors are running
    - whether msgr v1 or v2 addresses are used for the monitors
    - whether IPv4 or IPv6 addresses are used for the monitors
    - whether RADOS cache tiering is enabled (and which mode)
    - whether pools are replicated or erasure coded, and
      which erasure code profile plugin and parameters are in use
    - how many hosts are in the cluster, and how many hosts have each type of daemon
    - whether a separate OSD cluster network is being used
    - how many RBD pools and images are in the cluster, and how many pools have RBD mirroring enabled
    - how many RGW daemons, zones, and zonegroups are present; which RGW frontends are in use
    - aggregate stats about the CRUSH map, like which algorithms are used, how
      big buckets are, how many rules are defined, and what tunables are in
      use

  If you had telemetry enabled, you will need to re-opt-in with::

    ceph telemetry on

  You can view exactly what information will be reported first with::

    ceph telemetry show        # see everything
    ceph telemetry show basic  # basic cluster info (including all of the new info)

OSD:

* A new OSD daemon command, 'dump_recovery_reservations', reveals the
  recovery locks held (in_progress) and waiting in priority queues.

* Another new OSD daemon command, 'dump_scrub_reservations', reveals the
  scrub reservations that are held for local (primary) and remote (replica) PGs.

RGW:

* RGW now supports S3 Object Lock set of APIs allowing for a WORM model for
  storing objects. 6 new APIs have been added put/get bucket object lock,
  put/get object retention, put/get object legal hold.

* RGW now supports List Objects V2
        
Changelog
---------

* bluestore/KernelDevice: fix RW_IO_MAX constant (`pr#31397 <https://github.com/ceph/ceph/pull/31397>`_, Sage Weil)
* bluestore: Don't forget sub kv_submitted_waiters (`pr#30048 <https://github.com/ceph/ceph/pull/30048>`_, Jianpeng Ma)
* bluestore: apply garbage collection against excessive blob count growth (`pr#30144 <https://github.com/ceph/ceph/pull/30144>`_, Igor Fedotov)
* bluestore: apply shared_alloc_size to shared device with log level change (`pr#30229 <https://github.com/ceph/ceph/pull/30229>`_, Vikhyat Umrao, Sage Weil, Igor Fedotov, Neha Ojha)
* bluestore: consolidate extents from the same device only (`pr#31644 <https://github.com/ceph/ceph/pull/31644>`_, Igor Fedotov)
* bluestore: fix improper setting of STATE_KV_SUBMITTED (`pr#30755 <https://github.com/ceph/ceph/pull/30755>`_, Igor Fedotov)
* bluestore: shallow fsck mode and legacy statfs auto repair (`pr#30685 <https://github.com/ceph/ceph/pull/30685>`_, Sage Weil, Igor Fedotov)
* bluestore: tool to check fragmentation (`pr#29949 <https://github.com/ceph/ceph/pull/29949>`_, Adam Kupczyk)
* build/ops: admin/build-doc: use python3 (`pr#30664 <https://github.com/ceph/ceph/pull/30664>`_, Kefu Chai)
* build/ops: backport endian fixes (`issue#40114 <http://tracker.ceph.com/issues/40114>`_, `pr#30697 <https://github.com/ceph/ceph/pull/30697>`_, Ulrich Weigand, Jeff Layton)
* build/ops: cmake,rgw: IBM Z build fixes (`pr#30696 <https://github.com/ceph/ceph/pull/30696>`_, Ulrich Weigand)
* build/ops: cmake/BuildDPDK: ignore gcc8/9 warnings (`pr#30360 <https://github.com/ceph/ceph/pull/30360>`_, Yuval Lifshitz)
* build/ops: cmake: Allow cephfs and ceph-mds to be build when building on FreeBSD (`pr#31011 <https://github.com/ceph/ceph/pull/31011>`_, Willem Jan Withagen)
* build/ops: cmake: enforce C++17 instead of relying on cmake-compile-features (`pr#30283 <https://github.com/ceph/ceph/pull/30283>`_, Kefu Chai)
* build/ops: fix build fail related to PYTHON_EXECUTABLE variable (`pr#30261 <https://github.com/ceph/ceph/pull/30261>`_, Ilsoo Byun)
* build/ops: hidden corei7 requirement in binary packages (`pr#29772 <https://github.com/ceph/ceph/pull/29772>`_, Kefu Chai)
* build/ops: install-deps.sh: add EPEL repo for non-x86_64 archs as well (`pr#30601 <https://github.com/ceph/ceph/pull/30601>`_, Kefu Chai, Nathan Cutler)
* build/ops: install-deps.sh: install `python\*-devel` for python\*rpm-macros (`pr#30322 <https://github.com/ceph/ceph/pull/30322>`_, Kefu Chai)
* build/ops: install-deps: do not install if rpm already installed and ceph.spec.in: s/pkgversion/version_nodots/ (`pr#30708 <https://github.com/ceph/ceph/pull/30708>`_, Jeff Layton, Kefu Chai)
* build/ops: make patch build dependency explicit (`issue#40175 <http://tracker.ceph.com/issues/40175>`_, `pr#30046 <https://github.com/ceph/ceph/pull/30046>`_, Nathan Cutler)
* build/ops: python3-cephfs should provide python36-cephfs (`pr#30983 <https://github.com/ceph/ceph/pull/30983>`_, Kefu Chai)
* build/ops: rpm: always build ceph-test package (`pr#30049 <https://github.com/ceph/ceph/pull/30049>`_, Nathan Cutler)
* build/ops: rpm: fdupes in SUSE builds to conform with packaging guidelines (`issue#40973 <http://tracker.ceph.com/issues/40973>`_, `pr#29784 <https://github.com/ceph/ceph/pull/29784>`_, Nathan Cutler)
* build/ops: rpm: make librados2, libcephfs2 own (create) /etc/ceph (`pr#31125 <https://github.com/ceph/ceph/pull/31125>`_, Nathan Cutler)
* build/ops: rpm: put librgw lttng SOs in the librgw-devel package (`issue#40975 <http://tracker.ceph.com/issues/40975>`_, `pr#29785 <https://github.com/ceph/ceph/pull/29785>`_, Nathan Cutler)
* build/ops: seastar,dmclock: use CXX_FLAGS from parent project (`pr#30114 <https://github.com/ceph/ceph/pull/30114>`_, Kefu Chai)
* build/ops: use gcc-8 (`issue#38892 <http://tracker.ceph.com/issues/38892>`_, `pr#30089 <https://github.com/ceph/ceph/pull/30089>`_, Kefu Chai)
* tools: ceph-objectstore-tool: update-mon-db: do not fail if incmap is missing (`pr#30740 <https://github.com/ceph/ceph/pull/30740>`_, Kefu Chai)
* ceph-volume: PVolumes.filter shouldn't purge itself (`pr#30805 <https://github.com/ceph/ceph/pull/30805>`_, Rishabh Dave)
* ceph-volume: VolumeGroups.filter shouldn't purge itself (`pr#30807 <https://github.com/ceph/ceph/pull/30807>`_, Rishabh Dave)
* ceph-volume: add Ceph's device id to inventory (`pr#31210 <https://github.com/ceph/ceph/pull/31210>`_, Sebastian Wagner)
* ceph-volume: allow to skip restorecon calls (`pr#31555 <https://github.com/ceph/ceph/pull/31555>`_, Alfredo Deza)
* ceph-volume: api/lvm: check if list of LVs is empty (`pr#31228 <https://github.com/ceph/ceph/pull/31228>`_, Rishabh Dave)
* ceph-volume: check if we run in an selinux environment (`pr#31812 <https://github.com/ceph/ceph/pull/31812>`_, Jan Fajerski)
* ceph-volume: do not fail when trying to remove crypt mapper (`pr#30554 <https://github.com/ceph/ceph/pull/30554>`_, Guillaume Abrioux)
* ceph-volume: fix stderr failure to decode/encode when redirected (`pr#30300 <https://github.com/ceph/ceph/pull/30300>`_, Alfredo Deza)
* ceph-volume: fix warnings raised by pytest (`pr#30676 <https://github.com/ceph/ceph/pull/30676>`_, Rishabh Dave)
* ceph-volume: lvm list is O(n^2) (`pr#30093 <https://github.com/ceph/ceph/pull/30093>`_, Rishabh Dave)
* ceph-volume: lvm.zap fix cleanup for db partitions (`issue#40664 <http://tracker.ceph.com/issues/40664>`_, `pr#30304 <https://github.com/ceph/ceph/pull/30304>`_, Dominik Csapak)
* ceph-volume: mokeypatch calls to lvm related binaries (`pr#31405 <https://github.com/ceph/ceph/pull/31405>`_, Jan Fajerski)
* ceph-volume: pre-install python-apt and its variants before test runs (`pr#30294 <https://github.com/ceph/ceph/pull/30294>`_, Alfredo Deza)
* ceph-volume: rearrange api/lvm.py (`pr#31408 <https://github.com/ceph/ceph/pull/31408>`_, Rishabh Dave)
* ceph-volume: systemd fix typo in log message (`pr#30520 <https://github.com/ceph/ceph/pull/30520>`_, Manu Zurmühl)
* ceph-volume: use the OSD identifier when reporting success (`pr#29769 <https://github.com/ceph/ceph/pull/29769>`_, Alfredo Deza)
* ceph-volume: zap always skips block.db, leaves them around (`issue#40664 <http://tracker.ceph.com/issues/40664>`_, `pr#30307 <https://github.com/ceph/ceph/pull/30307>`_, Alfredo Deza)
* tools: ceph.in: do not preload ASan unless necessary (`pr#31676 <https://github.com/ceph/ceph/pull/31676>`_, Kefu Chai)
* build/ops: ceph.spec.in: reserve 2500MB per build job (`pr#30370 <https://github.com/ceph/ceph/pull/30370>`_, Dan van der Ster)
* tools: ceph_volume_client: convert string to bytes object (`issue#39405 <http://tracker.ceph.com/issues/39405>`_, `issue#40369 <http://tracker.ceph.com/issues/40369>`_, `issue#39510 <http://tracker.ceph.com/issues/39510>`_, `issue#40800 <http://tracker.ceph.com/issues/40800>`_, `issue#40460 <http://tracker.ceph.com/issues/40460>`_, `pr#30030 <https://github.com/ceph/ceph/pull/30030>`_, Rishabh Dave)
* cephfs-shell: Convert paths type from string to bytes (`pr#30057 <https://github.com/ceph/ceph/pull/30057>`_, Varsha Rao)
* cephfs: Allow mount.ceph to get mount info from ceph configs and keyrings (`pr#30521 <https://github.com/ceph/ceph/pull/30521>`_, Jeff Layton)
* cephfs: avoid map been inserted by mistake (`pr#29878 <https://github.com/ceph/ceph/pull/29878>`_, XiaoGuoDong2019)
* cephfs: client: more precise CEPH_CLIENT_CAPS_PENDING_CAPSNAP (`pr#30032 <https://github.com/ceph/ceph/pull/30032>`_, "Yan, Zheng")
* cephfs: client: nfs-ganesha with cephfs client, removing dir reports not empty (`issue#40746 <http://tracker.ceph.com/issues/40746>`_, `pr#30442 <https://github.com/ceph/ceph/pull/30442>`_, Peng Xie)
* cephfs: client: return -eio when sync file which unsafe reqs have been dropped (`issue#40877 <http://tracker.ceph.com/issues/40877>`_, `pr#30043 <https://github.com/ceph/ceph/pull/30043>`_, simon gao)
* cephfs: fix a memory leak (`pr#29879 <https://github.com/ceph/ceph/pull/29879>`_, XiaoGuoDong2019)
* cephfs: mds: Fix duplicate client entries in eviction list (`pr#30951 <https://github.com/ceph/ceph/pull/30951>`_, Sidharth Anupkrishnan)
* cephfs: mds: cleanup truncating inodes when standby replay mds trim log segments (`pr#29591 <https://github.com/ceph/ceph/pull/29591>`_, "Yan, Zheng")
* cephfs: mds: delay exporting directory whose pin value exceeds max rank id (`issue#40603 <http://tracker.ceph.com/issues/40603>`_, `pr#29938 <https://github.com/ceph/ceph/pull/29938>`_, Zhi Zhang)
* cephfs: mds: evict an unresponsive client only when another client wants its caps (`pr#30031 <https://github.com/ceph/ceph/pull/30031>`_, Rishabh Dave)
* cephfs: mds: fix InoTable::force_consume_to() (`pr#30041 <https://github.com/ceph/ceph/pull/30041>`_, "Yan, Zheng")
* cephfs: mds: fix infinite loop in Locker::file_update_finish (`pr#31079 <https://github.com/ceph/ceph/pull/31079>`_, "Yan, Zheng")
* cephfs: mds: make MDSIOContextBase delete itself when shutting down (`pr#30418 <https://github.com/ceph/ceph/pull/30418>`_, Xuehan Xu)
* cephfs: mds: trim cache on regular schedule (`pr#30040 <https://github.com/ceph/ceph/pull/30040>`_, Patrick Donnelly)
* cephfs: mds: wake up lock waiters after forcibly changing lock state (`issue#39987 <http://tracker.ceph.com/issues/39987>`_, `pr#30508 <https://github.com/ceph/ceph/pull/30508>`_, "Yan, Zheng")
* cephfs: mount.ceph: properly handle -o strictatime (`pr#30039 <https://github.com/ceph/ceph/pull/30039>`_, Jeff Layton)
* cephfs: qa: ignore expected MDS_CLIENT_LATE_RELEASE warning (`issue#40968 <http://tracker.ceph.com/issues/40968>`_, `pr#29811 <https://github.com/ceph/ceph/pull/29811>`_, Patrick Donnelly)
* cephfs: qa: wait for MDS to come back after removing it (`issue#40967 <http://tracker.ceph.com/issues/40967>`_, `pr#29832 <https://github.com/ceph/ceph/pull/29832>`_, Patrick Donnelly)
* cephfs: tests: power off still resulted in client sending session close (`issue#37681 <http://tracker.ceph.com/issues/37681>`_, `pr#29983 <https://github.com/ceph/ceph/pull/29983>`_, Patrick Donnelly)
* common/ceph_context: avoid unnecessary wait during service thread shutdown (`pr#31097 <https://github.com/ceph/ceph/pull/31097>`_, Jason Dillaman)
* common/config_proxy: hold lock while accessing mutable container (`pr#30661 <https://github.com/ceph/ceph/pull/30661>`_, Jason Dillaman)
* common: fix typo in rgw_user_max_buckets option long description (`pr#31605 <https://github.com/ceph/ceph/pull/31605>`_, Alfonso Martínez)
* core/osd: do not trust partially simplified pg_upmap_item (`issue#42052 <http://tracker.ceph.com/issues/42052>`_, `pr#30899 <https://github.com/ceph/ceph/pull/30899>`_, xie xingguo)
* core: Health warnings on long network ping times (`issue#40640 <http://tracker.ceph.com/issues/40640>`_, `pr#30195 <https://github.com/ceph/ceph/pull/30195>`_, David Zafman)
* core: If the nodeep-scrub/noscrub flags are set in pools instead of global cluster. List the pool names in the ceph status (`issue#38029 <http://tracker.ceph.com/issues/38029>`_, `pr#29991 <https://github.com/ceph/ceph/pull/29991>`_, Mohamad Gebai)
* core: Improve health status for backfill_toofull and recovery_toofull and fix backfill_toofull seen on cluster where the most full OSD is at 1% (`pr#29999 <https://github.com/ceph/ceph/pull/29999>`_, David Zafman)
* core: Make dumping of reservation info congruent between scrub and recovery (`pr#31444 <https://github.com/ceph/ceph/pull/31444>`_, David Zafman)
* core: Revert "rocksdb: enable rocksdb_rmrange=true by default" (`pr#31612 <https://github.com/ceph/ceph/pull/31612>`_, Neha Ojha)
* core: filestore pre-split may not split enough directories (`issue#39390 <http://tracker.ceph.com/issues/39390>`_, `pr#29988 <https://github.com/ceph/ceph/pull/29988>`_, Jeegn Chen)
* core: kv/RocksDBStore: tell rocksdb to set mode to 0600, not 0644 (`pr#31031 <https://github.com/ceph/ceph/pull/31031>`_, Sage Weil)
* core: mon/MonClient: ENXIO when sending command to down mon (`pr#31037 <https://github.com/ceph/ceph/pull/31037>`_, Sage Weil, Greg Farnum)
* core: mon/MonCommands: "smart" only needs read permission (`pr#31111 <https://github.com/ceph/ceph/pull/31111>`_, Kefu Chai)
* core: mon/MonMap: encode (more) valid compat monmap when we have v2-only addrs (`pr#31658 <https://github.com/ceph/ceph/pull/31658>`_, Sage Weil)
* core: mon/Monitor.cc: fix condition that checks for unrecognized auth mode (`pr#31038 <https://github.com/ceph/ceph/pull/31038>`_, Neha Ojha)
* core: mon/OSDMonitor: Use generic priority cache tuner for mon caches (`pr#30419 <https://github.com/ceph/ceph/pull/30419>`_, Sridhar Seshasayee, Kefu Chai, Mykola Golub, Mark Nelson)
* core: mon/OSDMonitor: add check for crush rule size in pool set size command (`pr#30941 <https://github.com/ceph/ceph/pull/30941>`_, Vikhyat Umrao)
* core: mon/OSDMonitor: trim not-longer-exist failure reporters (`pr#30904 <https://github.com/ceph/ceph/pull/30904>`_, NancySu05)
* core: mon/PGMap: fix incorrect pg_pool_sum when delete pool (`pr#31704 <https://github.com/ceph/ceph/pull/31704>`_, luo rixin)
* core: mon: C_AckMarkedDown has not handled the Callback Arguments (`pr#29997 <https://github.com/ceph/ceph/pull/29997>`_, NancySu05)
* core: mon: ensure prepare_failure() marks no_reply on op (`pr#30480 <https://github.com/ceph/ceph/pull/30480>`_, Joao Eduardo Luis)
* core: mon: show pool id in pool ls command (`issue#40287 <http://tracker.ceph.com/issues/40287>`_, `pr#30486 <https://github.com/ceph/ceph/pull/30486>`_, Chang Liu)
* core: msg,mon/MonClient: fix auth for clients without CEPHX_V2 feature (`pr#30524 <https://github.com/ceph/ceph/pull/30524>`_, Sage Weil)
* core: msg/auth: handle decode errors instead of throwing exceptions (`pr#31099 <https://github.com/ceph/ceph/pull/31099>`_, Sage Weil)
* core: msg/simple: reset in_seq_acked to zero when session is reset (`pr#29592 <https://github.com/ceph/ceph/pull/29592>`_, Xiangyang Yu)
* core: os/bluestore: fix objectstore_blackhole read-after-write (`pr#31019 <https://github.com/ceph/ceph/pull/31019>`_, Sage Weil)
* core: osd/OSDCap: Check for empty namespace (`issue#40835 <http://tracker.ceph.com/issues/40835>`_, `pr#29998 <https://github.com/ceph/ceph/pull/29998>`_, Brad Hubbard)
* core: mon/OSDMonitor: make memory autotune disable itself if no rocksdb (`pr#32045 <https://github.com/ceph/ceph/pull/32045>`_, Sage Weil)
* core: osd/PG: Add PG to large omap log message (`pr#30923 <https://github.com/ceph/ceph/pull/30923>`_, Brad Hubbard)
* core: osd/PGLog: persist num_objects_missing for replicas when peering is done (`pr#31077 <https://github.com/ceph/ceph/pull/31077>`_, xie xingguo)
* core: osd/PeeringState: do not complain about past_intervals constrained by oldest epoch (`pr#30000 <https://github.com/ceph/ceph/pull/30000>`_, Sage Weil)
* core: osd/PeeringState: fix wrong history of merge target (`pr#30280 <https://github.com/ceph/ceph/pull/30280>`_, xie xingguo)
* core: osd/PeeringState: recover_got - add special handler for empty log and improvements to standalone tests (`pr#30528 <https://github.com/ceph/ceph/pull/30528>`_, Sage Weil, David Zafman, xie xingguo)
* core: osd/PrimaryLogPG: Avoid accessing destroyed references in finish_degr… (`pr#29994 <https://github.com/ceph/ceph/pull/29994>`_, Tao Ning)
* core: osd/PrimaryLogPG: update oi.size on write op implicitly truncating ob… (`pr#30278 <https://github.com/ceph/ceph/pull/30278>`_, xie xingguo)
* core: osd/ReplicatedBackend: check against empty data_included before enabling crc (`pr#29716 <https://github.com/ceph/ceph/pull/29716>`_, xie xingguo)
* core: osd/osd_types: fix {omap,hitset_bytes}_stats_invalid handling on spli… (`pr#30643 <https://github.com/ceph/ceph/pull/30643>`_, Sage Weil)
* core: osd: Better error message when OSD count is less than osd_pool_default_size (`issue#38617 <http://tracker.ceph.com/issues/38617>`_, `pr#29992 <https://github.com/ceph/ceph/pull/29992>`_, Kefu Chai, Sage Weil, zjh)
* core: osd: Remove unused osdmap flags full, nearfull from output (`pr#30900 <https://github.com/ceph/ceph/pull/30900>`_, David Zafman)
* core: osd: add log information to record the cause of do_osd_ops failure (`pr#30546 <https://github.com/ceph/ceph/pull/30546>`_, NancySu05)
* core: osd: clear PG_STATE_CLEAN when repair object (`pr#30050 <https://github.com/ceph/ceph/pull/30050>`_, Zengran Zhang)
* core: osd: fix possible crash on sending dynamic perf stats report (`pr#30648 <https://github.com/ceph/ceph/pull/30648>`_, Mykola Golub)
* core: osd: merge replica log on primary need according to replica log's crt (`pr#30051 <https://github.com/ceph/ceph/pull/30051>`_, Zengran Zhang)
* core: osd: prime splits/merges for any potential fabricated split/merge par… (`issue#38483 <http://tracker.ceph.com/issues/38483>`_, `pr#30371 <https://github.com/ceph/ceph/pull/30371>`_, xie xingguo)
* core: osd: release backoffs during merge (`pr#31822 <https://github.com/ceph/ceph/pull/31822>`_, Sage Weil)
* core: osd: rollforward may need to mark pglog dirty (`issue#40403 <http://tracker.ceph.com/issues/40403>`_, `pr#31034 <https://github.com/ceph/ceph/pull/31034>`_, Zengran Zhang)
* core: osd: scrub error on big objects; make bluestore refuse to start on big objects (`pr#30783 <https://github.com/ceph/ceph/pull/30783>`_, David Zafman, Sage Weil)
* core: osd: support osd_repair_during_recovery (`issue#40620 <http://tracker.ceph.com/issues/40620>`_, `pr#29748 <https://github.com/ceph/ceph/pull/29748>`_, Jeegn Chen)
* core: pool_stat.dump() - value of num_store_stats is wrong (`issue#39340 <http://tracker.ceph.com/issues/39340>`_, `pr#29946 <https://github.com/ceph/ceph/pull/29946>`_, xie xingguo)
* doc/ceph-kvstore-tool: add description for 'stats' command (`pr#30245 <https://github.com/ceph/ceph/pull/30245>`_, Josh Durgin, Adam Kupczyk)
* doc/mgr/telemetry: update default interval (`pr#31009 <https://github.com/ceph/ceph/pull/31009>`_, Tim Serong)
* doc/rbd: s/guess/xml/ for codeblock lexer (`pr#31074 <https://github.com/ceph/ceph/pull/31074>`_, Kefu Chai)
* doc: Fix rbd namespace documentation (`pr#29731 <https://github.com/ceph/ceph/pull/29731>`_, Ricardo Marques)
* doc: cephfs: add section on fsync error reporting to posix.rst (`issue#24641 <http://tracker.ceph.com/issues/24641>`_, `pr#30025 <https://github.com/ceph/ceph/pull/30025>`_, Jeff Layton)
* doc: default values for mon_health_to_clog\_\* were flipped (`pr#30003 <https://github.com/ceph/ceph/pull/30003>`_, James McClune)
* doc: fix urls in posix.rst (`pr#30686 <https://github.com/ceph/ceph/pull/30686>`_, Jos Collin)
* doc: max_misplaced option was renamed in Nautilus (`pr#30649 <https://github.com/ceph/ceph/pull/30649>`_, Nathan Fish)
* doc: pg_num should always be a power of two (`pr#30004 <https://github.com/ceph/ceph/pull/30004>`_, Lars Marowsky-Bree, Kai Wagner)
* doc: update bluestore cache settings and clarify data fraction (`issue#39522 <http://tracker.ceph.com/issues/39522>`_, `pr#31259 <https://github.com/ceph/ceph/pull/31259>`_, Jan Fajerski)
* mgr/ActivePyModules: behave if a module queries a devid that does not exist (`pr#31411 <https://github.com/ceph/ceph/pull/31411>`_, Sage Weil)
* mgr/BaseMgrStandbyModule: drop GIL in ceph_get_module_option() (`pr#30773 <https://github.com/ceph/ceph/pull/30773>`_, Kefu Chai)
* mgr/balancer: python3 compatibility issue (`pr#31012 <https://github.com/ceph/ceph/pull/31012>`_, Mykola Golub)
* mgr/crash: backport archive feature, health alerts (`pr#30851 <https://github.com/ceph/ceph/pull/30851>`_, Sage Weil)
* mgr/crash: try client.crash[.host] before client.admin; add mon profile (`issue#40781 <http://tracker.ceph.com/issues/40781>`_, `pr#30844 <https://github.com/ceph/ceph/pull/30844>`_, Sage Weil, Dan Mick)
* mgr/dashboard: Add transifex-i18ntool (`pr#31160 <https://github.com/ceph/ceph/pull/31160>`_, Sebastian Krah)
* mgr/dashboard: Allow disabling redirection on standby dashboards (`issue#41813 <https://tracker.ceph.com/issues/41813>`_, `pr#30382 <https://github.com/ceph/ceph/pull/30382>`_, Volker Theile)
* mgr/dashboard: Configuring an URL prefix does not work as expected (`pr#31375 <https://github.com/ceph/ceph/pull/31375>`_, Volker Theile)
* mgr/dashbaord: Fix calculation of PG status percentage (`issue#41809 <https://tracker.ceph.com/issues/41089>`_, `pr#30394 <https://github.com/ceph/ceph/pull/30394>`_, Tiago Melo)
* mgr/dashboard: Fix CephFS chart (`pr#30691 <https://github.com/ceph/ceph/pull/30691>`_, Stephan Müller)
* mgr/dashboard: Fix grafana dashboards (`pr#31733 <https://github.com/ceph/ceph/pull/31733>`_, Radu Toader)
* mgr/dashboard: Improve position of MDS chart tooltip (`pr#31565 <https://github.com/ceph/ceph/pull/31565>`_, Tiago Melo)
* mgr/dashboard: Provide the name of the object being deleted (`pr#31263 <https://github.com/ceph/ceph/pull/31263>`_, Ricardo Marques)
* mgr/dashboard: RBD tests must use pools with power-of-two pg_num (`pr#31522 <https://github.com/ceph/ceph/pull/31522>`_, Ricardo Marques)
* mgr/dashboard: Set RO as the default access_type for RGW NFS exports (`pr#30516 <https://github.com/ceph/ceph/pull/30516>`_, Tiago Melo)
* mgr/dashboard: Wait for breadcrumb text is present in e2e tests (`pr#31576 <https://github.com/ceph/ceph/pull/31576>`_, Volker Theile)
* mgr/dashboard: access_control: add grafana scope read access to \*-manager roles (`pr#30259 <https://github.com/ceph/ceph/pull/30259>`_, Ricardo Dias)
* mgr/dashboard: do not log tokens (`pr#31413 <https://github.com/ceph/ceph/pull/31413>`_, Kefu Chai)
* mgr/dashboard: do not show non-pool data in pool details (`pr#31516 <https://github.com/ceph/ceph/pull/31516>`_, Alfonso Martínez)
* mgr/dashboard: edit/clone/copy rbd image after its data is received (`pr#31349 <https://github.com/ceph/ceph/pull/31349>`_, Alfonso Martínez)
* mgr/dashboard: internationalization support with AOT enabled (`pr#30910 <https://github.com/ceph/ceph/pull/30910>`_, Ricardo Dias, Tiago Melo)
* mgr/dashboard: run-backend-api-tests.sh improvements (`pr#29487 <https://github.com/ceph/ceph/pull/29487>`_, Alfonso Martínez, Kefu Chai)
* mgr/dashboard: tasks: only unblock controller thread after TaskManager thread (`pr#31526 <https://github.com/ceph/ceph/pull/31526>`_, Ricardo Dias)
* mgr/devicehealth: do not scrape mon devices (`pr#31446 <https://github.com/ceph/ceph/pull/31446>`_, Sage Weil)
* mgr/devicehealth: import _strptime directly (`pr#32082 <https://github.com/ceph/ceph/pull/32082>`_, Sage Weil)
* mgr/k8sevents: Initial ceph -> k8s events integration (`pr#30215 <https://github.com/ceph/ceph/pull/30215>`_, Paul Cuzner, Sebastian Wagner)
* mgr/pg_autoscaler: fix pool_logical_used (`pr#31100 <https://github.com/ceph/ceph/pull/31100>`_, Ansgar Jazdzewski)
* mgr/pg_autoscaler: fix race with pool deletion (`pr#30008 <https://github.com/ceph/ceph/pull/30008>`_, Sage Weil)
* mgr/prometheus: Cast collect_timeout (scrape_interval) to float (`pr#30007 <https://github.com/ceph/ceph/pull/30007>`_, Ben Meekhof)
* mgr/prometheus: Fix KeyError in get_mgr_status (`pr#30774 <https://github.com/ceph/ceph/pull/30774>`_, Sebastian Wagner)
* mgr/rbd_support: module.py:1088: error: Name 'image_spec' is not defined (`pr#29978 <https://github.com/ceph/ceph/pull/29978>`_, Jason Dillaman)
* mgr/restful: requests api adds support multiple commands (`pr#31334 <https://github.com/ceph/ceph/pull/31334>`_, Duncan Chiang)
* mgr/telemetry: backport a ton of stuff (`pr#30849 <https://github.com/ceph/ceph/pull/30849>`_, alfonsomthd, Kefu Chai, Sage Weil, Dan Mick)
* mgr/volumes: fix incorrect snapshot path creation (`pr#31076 <https://github.com/ceph/ceph/pull/31076>`_, Ramana Raja)
* mgr/volumes: handle exceptions in purge thread with retry (`issue#41218 <http://tracker.ceph.com/issues/41218>`_, `pr#30455 <https://github.com/ceph/ceph/pull/30455>`_, Venky Shankar)
* mgr/volumes: list FS subvolumes, subvolume groups, and their snapshots (`pr#30827 <https://github.com/ceph/ceph/pull/30827>`_, Jos Collin)
* mgr/volumes: minor fixes (`pr#29926 <https://github.com/ceph/ceph/pull/29926>`_, Venky Shankar, Jos Collin, Ramana Raja)
* mgr/volumes: protection for "fs volume rm" command (`pr#30768 <https://github.com/ceph/ceph/pull/30768>`_, Jos Collin, Ramana Raja)
* mgr/zabbix: Fix typo in key name for PGs in backfill_wait state (`issue#39666 <http://tracker.ceph.com/issues/39666>`_, `pr#30006 <https://github.com/ceph/ceph/pull/30006>`_, Wido den Hollander)
* mgr/zabbix: encode string for Python 3 compatibility (`pr#30016 <https://github.com/ceph/ceph/pull/30016>`_, Nathan Cutler)
* mgr/{dashboard,prometheus}: return FQDN instead of '0.0.0.0' (`pr#31482 <https://github.com/ceph/ceph/pull/31482>`_, Patrick Seidensal)
* mgr: Release GIL before calling OSDMap::calc_pg_upmaps() (`pr#31682 <https://github.com/ceph/ceph/pull/31682>`_, David Zafman, Shyukri Shyukriev)
* mgr: Unable to reset / unset module options (`issue#40779 <http://tracker.ceph.com/issues/40779>`_, `pr#29550 <https://github.com/ceph/ceph/pull/29550>`_, Sebastian Wagner)
* mgr: do not reset reported if a new metric is not collected (`pr#30390 <https://github.com/ceph/ceph/pull/30390>`_, Ilsoo Byun)
* mgr: fix weird health-alert daemon key (`pr#31039 <https://github.com/ceph/ceph/pull/31039>`_, xie xingguo)
* mgr: set hostname in DeviceState::set_metadata() (`pr#30624 <https://github.com/ceph/ceph/pull/30624>`_, Kefu Chai)
* pybind/cephfs: Modification to error message (`pr#30026 <https://github.com/ceph/ceph/pull/30026>`_, Varsha Rao)
* pybind/rados: fix set_omap() crash on py3 (`pr#30622 <https://github.com/ceph/ceph/pull/30622>`_, Sage Weil)
* pybind/rbd: deprecate `parent_info` (`pr#30818 <https://github.com/ceph/ceph/pull/30818>`_, Ricardo Marques)
* rbd: rbd-mirror: cannot restore deferred deletion mirrored images (`pr#30825 <https://github.com/ceph/ceph/pull/30825>`_, Jason Dillaman, Mykola Golub)
* rbd: rbd-mirror: don't overwrite status error returned by replay (`pr#29870 <https://github.com/ceph/ceph/pull/29870>`_, Mykola Golub)
* rbd: rbd-mirror: ignore errors relating to parsing the cluster config file (`pr#30116 <https://github.com/ceph/ceph/pull/30116>`_, Jason Dillaman)
* rbd: rbd-mirror: simplify peer bootstrapping (`pr#30821 <https://github.com/ceph/ceph/pull/30821>`_, Jason Dillaman)
* rbd: rbd-nbd: add netlink support and nl resize (`pr#30532 <https://github.com/ceph/ceph/pull/30532>`_, Mike Christie)
* rbd: cls/rbd: sanitize entity instance messenger version type (`pr#30822 <https://github.com/ceph/ceph/pull/30822>`_, Jason Dillaman)
* rbd: cls/rbd: sanitize the mirror image status peer address after reading from disk (`pr#31833 <https://github.com/ceph/ceph/pull/31833>`_, Jason Dillaman)
* rbd: krbd: avoid udev netlink socket overrun and retry on transient errors from udev_enumerate_scan_devices() (`pr#31075 <https://github.com/ceph/ceph/pull/31075>`_, Ilya Dryomov, Adam C. Emerson)
* rbd: librbd: always try to acquire exclusive lock when removing image (`pr#29869 <https://github.com/ceph/ceph/pull/29869>`_, Mykola Golub)
* rbd: librbd: behave more gracefully when data pool removed (`pr#30824 <https://github.com/ceph/ceph/pull/30824>`_, Mykola Golub)
* rbd: librbd: v1 clones are restricted to the same namespace (`pr#30823 <https://github.com/ceph/ceph/pull/30823>`_, Jason Dillaman)
* mgr/restful: Query nodes_by_id for items (`pr#31261 <https://github.com/ceph/ceph/pull/31261>`_, Boris Ranto)
* rgw/amqp: fix race condition in AMQP unit test (`pr#30889 <https://github.com/ceph/ceph/pull/30889>`_, Yuval Lifshitz)
* rgw/amqp: remove flaky amqp test (`pr#31628 <https://github.com/ceph/ceph/pull/31628>`_, Yuval Lifshitz)
* rgw/pubsub: backport notifications and pubsub (`pr#30579 <https://github.com/ceph/ceph/pull/30579>`_, Yuval Lifshitz)
* rgw/rgw_op: Remove get_val from hotpath via legacy options (`pr#30160 <https://github.com/ceph/ceph/pull/30160>`_, Mark Nelson)
* rgw: Potential crash in putbj (`pr#29898 <https://github.com/ceph/ceph/pull/29898>`_, Adam C. Emerson)
* rgw: Put User Policy is sensitive to whitespace (`pr#29970 <https://github.com/ceph/ceph/pull/29970>`_, Abhishek Lekshmanan)
* rgw: RGWCoroutine::call(nullptr) sets retcode=0 (`pr#30248 <https://github.com/ceph/ceph/pull/30248>`_, Casey Bodley)
* rgw: Swift metadata dropped after S3 bucket versioning enabled (`pr#29961 <https://github.com/ceph/ceph/pull/29961>`_, Marcus Watts)
* rgw: add S3 object lock feature to support object worm (`pr#29905 <https://github.com/ceph/ceph/pull/29905>`_, Chang Liu, Casey Bodley, zhang Shaowen)
* rgw: add minssing admin property when sync user info (`pr#30680 <https://github.com/ceph/ceph/pull/30680>`_, zhang Shaowen)
* rgw: beast frontend throws an exception when running out of FDs (`pr#29963 <https://github.com/ceph/ceph/pull/29963>`_, Yuval Lifshitz)
* rgw: data/bilogs are trimmed when no peers are reading them (`issue#39487 <http://tracker.ceph.com/issues/39487>`_, `pr#30999 <https://github.com/ceph/ceph/pull/30999>`_, Casey Bodley)
* rgw: datalog/mdlog trim commands loop until done (`pr#30869 <https://github.com/ceph/ceph/pull/30869>`_, Casey Bodley)
* rgw: dns name is not case sensitive (`issue#40995 <http://tracker.ceph.com/issues/40995>`_, `pr#29971 <https://github.com/ceph/ceph/pull/29971>`_, Casey Bodley, Abhishek Lekshmanan)
* rgw: fix a bug that lifecycle expiraton generates delete marker continuously (`issue#40393 <http://tracker.ceph.com/issues/40393>`_, `pr#30037 <https://github.com/ceph/ceph/pull/30037>`_, zhang Shaowen)
* rgw: fix cls_bucket_list_unordered() partial results (`pr#30252 <https://github.com/ceph/ceph/pull/30252>`_, Mark Kogan)
* rgw: fix data sync start delay if remote haven't init data_log (`pr#30509 <https://github.com/ceph/ceph/pull/30509>`_, Tianshan Qu)
* rgw: fix default storage class for get_compression_type (`pr#31026 <https://github.com/ceph/ceph/pull/31026>`_, Casey Bodley)
* rgw: fix drain handles error when deleting bucket with bypass-gc option (`pr#29956 <https://github.com/ceph/ceph/pull/29956>`_, dongdong tao)
* rgw: fix list bucket with delimiter wrongly skip some special keys (`issue#40905 <http://tracker.ceph.com/issues/40905>`_, `pr#30068 <https://github.com/ceph/ceph/pull/30068>`_, Tianshan Qu)
* rgw: fix memory growth while deleteing objects with (`pr#30472 <https://github.com/ceph/ceph/pull/30472>`_, Mark Kogan)
* rgw: fix the bug of rgw not doing necessary checking to website configuration (`issue#40678 <http://tracker.ceph.com/issues/40678>`_, `pr#30325 <https://github.com/ceph/ceph/pull/30325>`_, Enming Zhang)
* rgw: fixed "unrecognized arg" error when using "radosgw-admin zone rm" (`pr#30247 <https://github.com/ceph/ceph/pull/30247>`_, Hongang Chen)
* rgw: housekeeping reset stats (`pr#29803 <https://github.com/ceph/ceph/pull/29803>`_, J. Eric Ivancich)
* rgw: increase beast parse buffer size to 64k (`pr#30437 <https://github.com/ceph/ceph/pull/30437>`_, Casey Bodley)
* rgw: ldap auth: S3 auth failure should return InvalidAccessKeyId (`pr#30651 <https://github.com/ceph/ceph/pull/30651>`_, Matt Benjamin)
* rgw: lifecycle days may be 0 (`pr#31073 <https://github.com/ceph/ceph/pull/31073>`_, Matt Benjamin)
* rgw: lifecycle transitions on non existent placement targets (`pr#29955 <https://github.com/ceph/ceph/pull/29955>`_, Abhishek Lekshmanan)
* rgw: list objects version 2 (`pr#29849 <https://github.com/ceph/ceph/pull/29849>`_, Albin Antony, zhang Shaowen)
* rgw: multisite: radosgw-admin bucket sync status incorrectly reports "caught up" during full sync (`issue#40806 <http://tracker.ceph.com/issues/40806>`_, `pr#29974 <https://github.com/ceph/ceph/pull/29974>`_, Casey Bodley)
* rgw: potential realm watch lost (`issue#40991 <http://tracker.ceph.com/issues/40991>`_, `pr#29972 <https://github.com/ceph/ceph/pull/29972>`_, Tianshan Qu)
* rgw: protect AioResultList by a lock to avoid race condition (`pr#30746 <https://github.com/ceph/ceph/pull/30746>`_, Ilsoo Byun)
* rgw: radosgw-admin: add --uid check in bucket list command (`pr#30604 <https://github.com/ceph/ceph/pull/30604>`_, Vikhyat Umrao)
* rgw: returns one byte more data than the requested range from the SLO object (`pr#29960 <https://github.com/ceph/ceph/pull/29960>`_, Andrey Groshev)
* rgw: rgw-admin: search for user by access key (`pr#29959 <https://github.com/ceph/ceph/pull/29959>`_, Matt Benjamin)
* rgw: rgw-log issues the wrong message when decompression fails (`pr#29965 <https://github.com/ceph/ceph/pull/29965>`_, Han Fengzhe)
* rgw: rgw_file: directory enumeration can be accelerated 1-2 orders of magnitude taking stats from bucket index Part I (stats from S3/Swift only) (`issue#40456 <http://tracker.ceph.com/issues/40456>`_, `pr#29954 <https://github.com/ceph/ceph/pull/29954>`_, Matt Benjamin)
* rgw: rgw_file: readdir: do not construct markers w/leading '/' (`pr#29969 <https://github.com/ceph/ceph/pull/29969>`_, Matt Benjamin)
* rgw: silence warning "control reaches end of non-void function" (`issue#40747 <http://tracker.ceph.com/issues/40747>`_, `pr#31742 <https://github.com/ceph/ceph/pull/31742>`_, Jos Collin)
* rgw: sync with elastic search v7 (`pr#31027 <https://github.com/ceph/ceph/pull/31027>`_, Chang Liu)
* rgw: use explicit to_string() overload for boost::string_ref (`issue#39611 <http://tracker.ceph.com/issues/39611>`_, `pr#31650 <https://github.com/ceph/ceph/pull/31650>`_, Casey Bodley, Ulrich Weigand)
* rgw: when using radosgw-admin to list bucket, can set --max-entries excessively high (`pr#29777 <https://github.com/ceph/ceph/pull/29777>`_, J. Eric Ivancich)
* tests: "CMake Error" in test_envlibrados_for_rocksdb.sh (`pr#29979 <https://github.com/ceph/ceph/pull/29979>`_, Kefu Chai)
* tests: Get libcephfs and cephfs to compile with FreeBSD (`pr#31136 <https://github.com/ceph/ceph/pull/31136>`_, Willem Jan Withagen)
* tests: add debugging failed osd-release setting (`pr#31040 <https://github.com/ceph/ceph/pull/31040>`_, Patrick Donnelly)
* tests: cephfs: fix malformed qa suite config (`pr#30038 <https://github.com/ceph/ceph/pull/30038>`_, Patrick Donnelly)
* tests: cls_rbd/test_cls_rbd: update TestClsRbd.sparsify (`pr#30354 <https://github.com/ceph/ceph/pull/30354>`_, Kefu Chai)
* tests: cls_rbd: removed mirror peer pool test cases (`pr#30948 <https://github.com/ceph/ceph/pull/30948>`_, Jason Dillaman)
* tests: enable dashboard tests to be run with "--suite rados/dashboard" (`pr#31248 <https://github.com/ceph/ceph/pull/31248>`_, Nathan Cutler)
* tests: librbd: set nbd timeout due to newer kernels defaulting it on (`pr#30423 <https://github.com/ceph/ceph/pull/30423>`_, Jason Dillaman)
* tests: qa/suites/krbd: run unmap subsuite with msgr1 only (`pr#31290 <https://github.com/ceph/ceph/pull/31290>`_, Ilya Dryomov)
* tests: qa/tasks/cbt: run stop-all.sh while shutting down (`pr#31304 <https://github.com/ceph/ceph/pull/31304>`_, Sage Weil)
* tests: qa/tasks/ceph.conf.template: increase mon tell retries (`pr#31641 <https://github.com/ceph/ceph/pull/31641>`_, Sage Weil)
* tests: qa/workunits/rbd: stress test `rbd mirror pool status --verbose` (`pr#29871 <https://github.com/ceph/ceph/pull/29871>`_, Mykola Golub)
* tests: qa: avoid page cache for krbd discard round off tests (`pr#30464 <https://github.com/ceph/ceph/pull/30464>`_, Ilya Dryomov)
* tests: qa: sleep briefly after resetting kclient (`pr#29750 <https://github.com/ceph/ceph/pull/29750>`_, Patrick Donnelly)
* tests: rados/mgr/tasks/module_selftest: whitelist mgr client getting blacklisted (`issue#40867 <http://tracker.ceph.com/issues/40867>`_, `pr#29649 <https://github.com/ceph/ceph/pull/29649>`_, Sage Weil)
* tests: test_librados_build.sh: grab from nautilus branch in nautilus (`pr#31604 <https://github.com/ceph/ceph/pull/31604>`_, Nathan Cutler)
* tests: valgrind: UninitCondition in ceph::crypto::onwire::AES128GCM_OnWireRxHandler::authenticated_decrypt_update_final() (`issue#38827 <http://tracker.ceph.com/issues/38827>`_, `pr#29928 <https://github.com/ceph/ceph/pull/29928>`_, Radoslaw Zarzynski)
* tools/rados: add --pgid in help (`pr#30607 <https://github.com/ceph/ceph/pull/30607>`_, Vikhyat Umrao)
* tools/rados: call pool_lookup() after rados is connected (`pr#30605 <https://github.com/ceph/ceph/pull/30605>`_, Vikhyat Umrao)
* tools/rbd-ggate: close log before running postfork (`pr#30120 <https://github.com/ceph/ceph/pull/30120>`_, Willem Jan Withagen)
* tools: ceph-backport.sh: add deprecation warning (`pr#30748 <https://github.com/ceph/ceph/pull/30748>`_, Nathan Cutler)
* tools: ceph-objectstore-tool can't remove head with bad snapset (`pr#30080 <https://github.com/ceph/ceph/pull/30080>`_, David Zafman)


v14.2.4 Nautilus
================

This is the fourth release in the Ceph Nautilus stable release series. Its sole
purpose is to fix a regression that found its way into the previous release.

Notable Changes
---------------

* The ceph-volume in Nautilus v14.2.3 was found to contain a serious
  regression, described in ``https://tracker.ceph.com/issues/41660``, which 
  prevented deployment tools like ceph-ansible, DeepSea, Rook, etc. from 
  deploying/removing OSDs.

Changelog
---------

* ceph-volume: fix stderr failure to decode/encode when redirected (`pr#30300 <https://github.com/ceph/ceph/pull/30300>`_, Alfredo Deza)


v14.2.3 Nautilus
================

This is the third bug fix release of Ceph Nautilus release series. We recommend
all Nautilus users upgrade to this release. For upgrading from older releases of
ceph, general guidelines for upgrade to nautilus must be followed
:ref:`nautilus-old-upgrade`.

Notable Changes
---------------

* `CVE-2019-10222` - Fixed a denial of service vulnerability where an
  unauthenticated client of Ceph Object Gateway could trigger a crash from an
  uncaught exception

* Nautilus-based librbd clients can now open images on Jewel clusters.

* The RGW `num_rados_handles` has been removed. If you were using a value of
  `num_rados_handles` greater than 1, multiply your current
  `objecter_inflight_ops` and `objecter_inflight_op_bytes` parameters by the
  old `num_rados_handles` to get the same throttle behavior.

* The secure mode of Messenger v2 protocol is no longer experimental with this
  release. This mode is now the preferred mode of connection for monitors.

* "osd_deep_scrub_large_omap_object_key_threshold" has been lowered to detect an
  object with large number of omap keys more easily.

* The Ceph Dashboard now supports silencing Prometheus alert notifications.

Changelog
---------

* bluestore: 50-100% iops lost due to bluefs_preextend_wal_files = false (`issue#38559 <http://tracker.ceph.com/issues/38559>`_, `pr#28573 <https://github.com/ceph/ceph/pull/28573>`_, Vitaliy Filippov)
* bluestore: add slow op detection for collection_listing (`pr#29227 <https://github.com/ceph/ceph/pull/29227>`_, Igor Fedotov)
* bluestore: avoid length overflow in extents returned by Stupid Allocator (`issue#40703 <http://tracker.ceph.com/issues/40703>`_, `pr#29023 <https://github.com/ceph/ceph/pull/29023>`_, Igor Fedotov)
* bluestore/bluefs_types: consolidate contiguous extents (`pr#28862 <https://github.com/ceph/ceph/pull/28862>`_, Sage Weil)
* bluestore/bluestore-tool: minor fixes around migrate (`pr#28893 <https://github.com/ceph/ceph/pull/28893>`_, Igor Fedotov)
* bluestore: create the tail when first set FLAG_OMAP (`issue#36482 <http://tracker.ceph.com/issues/36482>`_, `pr#28963 <https://github.com/ceph/ceph/pull/28963>`_, Tao Ning)
* bluestore: do not set osd_memory_target default from cgroup limit (`pr#29745 <https://github.com/ceph/ceph/pull/29745>`_, Sage Weil)
* bluestore: fix >2GB bluefs writes (`pr#28966 <https://github.com/ceph/ceph/pull/28966>`_, kungf, Sage Weil)
* bluestore: load OSD all compression settings unconditionally (`issue#40480 <http://tracker.ceph.com/issues/40480>`_, `pr#28892 <https://github.com/ceph/ceph/pull/28892>`_, Igor Fedotov)
* bluestore: more smart allocator dump when lacking space for bluefs (`issue#40623 <http://tracker.ceph.com/issues/40623>`_, `pr#28891 <https://github.com/ceph/ceph/pull/28891>`_, Igor Fedotov)
* bluestore: Set concurrent max_background_compactions in rocksdb to 2 (`issue#40769 <http://tracker.ceph.com/issues/40769>`_, `pr#29162 <https://github.com/ceph/ceph/pull/29162>`_, Mark Nelson)
* bluestore: support RocksDB prefetch in buffered read mode (`pr#28962 <https://github.com/ceph/ceph/pull/28962>`_, Igor Fedotov)
* build/ops: Module 'dashboard' has failed: No module named routes (`issue#24420 <http://tracker.ceph.com/issues/24420>`_, `pr#28992 <https://github.com/ceph/ceph/pull/28992>`_, Paul Emmerich)
* build/ops: rpm: drop SuSEfirewall2 (`issue#40738 <http://tracker.ceph.com/issues/40738>`_, `pr#29007 <https://github.com/ceph/ceph/pull/29007>`_, Matthias Gerstner)
* build/ops: rpm: Require ceph-grafana-dashboards (`pr#29682 <https://github.com/ceph/ceph/pull/29682>`_, Boris Ranto)
* cephfs: ceph-fuse: mount does not support the fallocate() (`issue#40615 <http://tracker.ceph.com/issues/40615>`_, `pr#29157 <https://github.com/ceph/ceph/pull/29157>`_, huanwen ren)
* cephfs: ceph_volume_client: d_name needs to be converted to string before using (`issue#39406 <http://tracker.ceph.com/issues/39406>`_, `pr#28609 <https://github.com/ceph/ceph/pull/28609>`_, Rishabh Dave)
* cephfs: client: bump ll_ref from int32 to uint64_t (`pr#29186 <https://github.com/ceph/ceph/pull/29186>`_, Xiaoxi CHEN)
* cephfs: client: set snapdir's link count to 1 (`issue#40101 <http://tracker.ceph.com/issues/40101>`_, `pr#29343 <https://github.com/ceph/ceph/pull/29343>`_, "Yan, Zheng")
* cephfs: client: unlink dentry for inode with llref=0 (`issue#40960 <http://tracker.ceph.com/issues/40960>`_, `pr#29478 <https://github.com/ceph/ceph/pull/29478>`_, Xiaoxi CHEN)
* cephfs: getattr on snap inode stuck (`issue#40361 <http://tracker.ceph.com/issues/40361>`_, `pr#29231 <https://github.com/ceph/ceph/pull/29231>`_, "Yan, Zheng")
* cephfs: mds: cannot switch mds state from standby-replay to active (`issue#40213 <http://tracker.ceph.com/issues/40213>`_, `pr#29233 <https://github.com/ceph/ceph/pull/29233>`_, simon gao)
* cephfs: mds: cleanup unneeded client_snap_caps when splitting snap inode (`issue#39987 <http://tracker.ceph.com/issues/39987>`_, `pr#29344 <https://github.com/ceph/ceph/pull/29344>`_, "Yan, Zheng")
* cephfs-shell: name 'files' is not defined error in do_rm() (`issue#40489 <http://tracker.ceph.com/issues/40489>`_, `pr#29158 <https://github.com/ceph/ceph/pull/29158>`_, Varsha Rao)
* cephfs-shell: TypeError in poutput (`issue#40679 <http://tracker.ceph.com/issues/40679>`_, `pr#29156 <https://github.com/ceph/ceph/pull/29156>`_, Varsha Rao)
* ceph.spec.in: Drop systemd BuildRequires in case of building for SUSE (`pr#28937 <https://github.com/ceph/ceph/pull/28937>`_, Dominique Leuenberger)
* ceph-volume: batch functional idempotency test fails since message is now on stderr (`pr#29689 <https://github.com/ceph/ceph/pull/29689>`_, Jan Fajerski)
* ceph-volume: batch gets confused when the same device is passed in two device lists (`pr#29690 <https://github.com/ceph/ceph/pull/29690>`_, Jan Fajerski)
* ceph-volume: does not recognize wal/db partitions created by ceph-disk (`pr#29464 <https://github.com/ceph/ceph/pull/29464>`_, Jan Fajerski)
* ceph-volume: [filestore,bluestore] single type strategies fail after tracking devices as sets (`pr#29702 <https://github.com/ceph/ceph/pull/29702>`_, Jan Fajerski)
* ceph-volume: lvm.activate: Return an error if WAL/DB devices absent (`pr#29040 <https://github.com/ceph/ceph/pull/29040>`_, David Casier)
* ceph-volume: missing string substitution when reporting mounts (`issue#25030 <http://tracker.ceph.com/issues/25030>`_, `pr#29260 <https://github.com/ceph/ceph/pull/29260>`_, Shyukri Shyukriev)
* ceph-volume: prints errors to stdout with --format json (`issue#38548 <http://tracker.ceph.com/issues/38548>`_, `pr#29506 <https://github.com/ceph/ceph/pull/29506>`_, Jan Fajerski)
* ceph-volume: prints log messages to stdout (`pr#29600 <https://github.com/ceph/ceph/pull/29600>`_, Jan Fajerski, Kefu Chai, Alfredo Deza)
* ceph-volume: run functional tests without dashboard (`pr#29694 <https://github.com/ceph/ceph/pull/29694>`_, Andrew Schoen)
* ceph-volume: simple functional tests drop test for lvm zap (`pr#29660 <https://github.com/ceph/ceph/pull/29660>`_, Jan Fajerski)
* ceph-volume: tests set the noninteractive flag for Debian (`pr#29899 <https://github.com/ceph/ceph/pull/29899>`_, Alfredo Deza)
* ceph-volume: when 'type' file is not present activate fails (`pr#29416 <https://github.com/ceph/ceph/pull/29416>`_, Alfredo Deza)
* cmake: update FindBoost.cmake (`pr#29436 <https://github.com/ceph/ceph/pull/29436>`_, Willem Jan Withagen)
* common/config: respect POD_MEMORY_REQUEST \*and\* POD_MEMORY_LIMIT env vars (`pr#29562 <https://github.com/ceph/ceph/pull/29562>`_, Patrick Donnelly, Sage Weil)
* common: Keyrings created by ceph auth get are not suitable for ceph auth import (`issue#22227 <http://tracker.ceph.com/issues/22227>`_, `pr#28740 <https://github.com/ceph/ceph/pull/28740>`_, Kefu Chai)
* common: OutputDataSocket retakes mutex on error path (`issue#40188 <http://tracker.ceph.com/issues/40188>`_, `pr#29147 <https://github.com/ceph/ceph/pull/29147>`_, Casey Bodley)
* core: Better default value for osd_snap_trim_sleep (`pr#29678 <https://github.com/ceph/ceph/pull/29678>`_, Neha Ojha)
* core: Change default for bluestore_fsck_on_mount_deep as false (`pr#29697 <https://github.com/ceph/ceph/pull/29697>`_, Neha Ojha)
* core: lazy omap stat collection (`pr#29188 <https://github.com/ceph/ceph/pull/29188>`_, Brad Hubbard)
* core: librados: move buffer free functions to inline namespace (`issue#39972 <http://tracker.ceph.com/issues/39972>`_, `pr#29244 <https://github.com/ceph/ceph/pull/29244>`_, Jason Dillaman)
* core: maybe_remove_pg_upmap can be super inefficient for large clusters (`issue#40104 <http://tracker.ceph.com/issues/40104>`_, `pr#28756 <https://github.com/ceph/ceph/pull/28756>`_, xie xingguo)
* core: MDSMonitor: use stringstream instead of dout for mds repaired (`issue#40472 <http://tracker.ceph.com/issues/40472>`_, `pr#29159 <https://github.com/ceph/ceph/pull/29159>`_, Zhi Zhang)
* core: osd beacon sometimes has empty pg list (`issue#40377 <http://tracker.ceph.com/issues/40377>`_, `pr#29254 <https://github.com/ceph/ceph/pull/29254>`_, Sage Weil)
* core: s3tests-test-readwrite failed in rados run (Connection refused) (`issue#17882 <http://tracker.ceph.com/issues/17882>`_, `pr#29325 <https://github.com/ceph/ceph/pull/29325>`_, Casey Bodley)
* doc: Document more cache modes (`issue#14153 <http://tracker.ceph.com/issues/14153>`_, `pr#28958 <https://github.com/ceph/ceph/pull/28958>`_, Nathan Cutler)
* doc: fix rgw ldap username token (`pr#29455 <https://github.com/ceph/ceph/pull/29455>`_, Thomas Kriechbaumer)
* doc: Improved dashboard feature overview (`pr#28919 <https://github.com/ceph/ceph/pull/28919>`_, Lenz Grimmer)
* doc: Object Gateway multisite document read-only argument error (`issue#40458 <http://tracker.ceph.com/issues/40458>`_, `pr#29306 <https://github.com/ceph/ceph/pull/29306>`_, Chenjiong Deng)
* doc/rados: Correcting some typos in the clay code documentation (`pr#29191 <https://github.com/ceph/ceph/pull/29191>`_, Myna)
* doc/rbd: initial live-migration documentation (`issue#40486 <http://tracker.ceph.com/issues/40486>`_, `pr#29724 <https://github.com/ceph/ceph/pull/29724>`_, Jason Dillaman)
* doc/rgw: document use of 'realm pull' instead of 'period pull' (`issue#39655 <http://tracker.ceph.com/issues/39655>`_, `pr#29484 <https://github.com/ceph/ceph/pull/29484>`_, Casey Bodley)
* doc: steps to disable metadata_heap on existing rgw zones (`issue#18174 <http://tracker.ceph.com/issues/18174>`_, `pr#28738 <https://github.com/ceph/ceph/pull/28738>`_, Dan van der Ster)
* doc: Update 'ceph-iscsi' min version (`pr#29444 <https://github.com/ceph/ceph/pull/29444>`_, Ricardo Marques)
* journal: properly advance read offset after skipping invalid range (`pr#28816 <https://github.com/ceph/ceph/pull/28816>`_, Mykola Golub)
* librbd: improve journal performance to match expected degredation (`issue#40072 <http://tracker.ceph.com/issues/40072>`_, `pr#29723 <https://github.com/ceph/ceph/pull/29723>`_, Mykola Golub, Jason Dillaman)
* librbd: properly track in-flight flush requests (`issue#40555 <http://tracker.ceph.com/issues/40555>`_, `pr#28769 <https://github.com/ceph/ceph/pull/28769>`_, Jason Dillaman)
* librbd: snapshot object maps can go inconsistent during copyup (`issue#39435 <http://tracker.ceph.com/issues/39435>`_, `pr#29722 <https://github.com/ceph/ceph/pull/29722>`_, Ilya Dryomov)
* mds: change how mds revoke stale caps (`issue#17854 <http://tracker.ceph.com/issues/17854>`_, `pr#28583 <https://github.com/ceph/ceph/pull/28583>`_, Rishabh Dave, "Yan, Zheng")
* mgr: Add mgr metdata to prometheus exporter module (`pr#29168 <https://github.com/ceph/ceph/pull/29168>`_, Paul Cuzner)
* mgr/dashboard: Add, update and remove translations (`issue#39701 <http://tracker.ceph.com/issues/39701>`_, `pr#28938 <https://github.com/ceph/ceph/pull/28938>`_, Sebastian Krah)
* mgr/dashboard: cephfs multimds graphs stack together (`issue#37579 <http://tracker.ceph.com/issues/37579>`_, `pr#28889 <https://github.com/ceph/ceph/pull/28889>`_, Kiefer Chang)
* mgr/dashboard: Changing rgw-api-host does not get effective without disable/enable dashboard mgr module (`issue#40252 <http://tracker.ceph.com/issues/40252>`_, `pr#29044 <https://github.com/ceph/ceph/pull/29044>`_, Ricardo Marques)
* mgr/dashboard: controllers/grafana is not Python3 compatible (`issue#40428 <http://tracker.ceph.com/issues/40428>`_, `pr#29524 <https://github.com/ceph/ceph/pull/29524>`_, Patrick Nawracay)
* mgr/dashboard: Dentries value of MDS daemon in Filesystems page is inconsistent with ceph fs status output (`issue#40097 <http://tracker.ceph.com/issues/40097>`_, `pr#28912 <https://github.com/ceph/ceph/pull/28912>`_, Kiefer Chang)
* mgr/dashboard: Display logged in information for each iSCSI client (`issue#40046 <http://tracker.ceph.com/issues/40046>`_, `pr#29045 <https://github.com/ceph/ceph/pull/29045>`_, Ricardo Marques)
* mgr/dashboard: Fix e2e failures caused by webdriver version (`pr#29491 <https://github.com/ceph/ceph/pull/29491>`_, Tiago Melo)
* mgr/dashboard: Fix npm vulnerabilities (`issue#40677 <http://tracker.ceph.com/issues/40677>`_, `pr#29102 <https://github.com/ceph/ceph/pull/29102>`_, Tiago Melo)
* mgr/dashboard: Fix the table mouseenter event handling test (`issue#40580 <http://tracker.ceph.com/issues/40580>`_, `pr#29354 <https://github.com/ceph/ceph/pull/29354>`_, Stephan Müller)
* mgr/dashboard: Interlock `fast-diff` and `object-map` (`issue#39451 <http://tracker.ceph.com/issues/39451>`_, `pr#29442 <https://github.com/ceph/ceph/pull/29442>`_, Patrick Nawracay)
* mgr/dashboard: notify the user about unset 'mon_allow_pool_delete' flag beforehand (`issue#39533 <http://tracker.ceph.com/issues/39533>`_, `pr#28833 <https://github.com/ceph/ceph/pull/28833>`_, Tatjana Dehler)
* mgr/dashboard: Optimize the calculation of portal IPs (`issue#39580 <http://tracker.ceph.com/issues/39580>`_, `pr#29061 <https://github.com/ceph/ceph/pull/29061>`_, Ricardo Marques, Kefu Chai)
* mgr/dashboard: Pool graph/sparkline points do not display the correct values (`issue#39650 <http://tracker.ceph.com/issues/39650>`_, `pr#29352 <https://github.com/ceph/ceph/pull/29352>`_, Stephan Müller)
* mgr/dashboard: RGW User quota validation is not working correctly (`pr#29650 <https://github.com/ceph/ceph/pull/29650>`_, Volker Theile)
* mgr/dashboard: Silence Alertmanager alerts (`issue#36722 <http://tracker.ceph.com/issues/36722>`_, `pr#28968 <https://github.com/ceph/ceph/pull/28968>`_, Stephan Müller)
* mgr/dashboard: SSL certificate upload command throws deprecation warning (`issue#39123 <http://tracker.ceph.com/issues/39123>`_, `pr#29065 <https://github.com/ceph/ceph/pull/29065>`_, Ricardo Dias)
* mgr/dashboard: switch ng2-toastr to ngx-toastr (`pr#29050 <https://github.com/ceph/ceph/pull/29050>`_, Tiago Melo, Ernesto Puerta)
* mgr/dashboard: Upgrade to ceph-iscsi config v10 (`issue#40566 <http://tracker.ceph.com/issues/40566>`_, `pr#28974 <https://github.com/ceph/ceph/pull/28974>`_, Ricardo Marques)
* mgr/diskprediction_cloud: Service unavailable (`issue#40478 <http://tracker.ceph.com/issues/40478>`_, `pr#29454 <https://github.com/ceph/ceph/pull/29454>`_, Rick Chen)
* mgr/influx: module fails due to missing close() method (`issue#40174 <http://tracker.ceph.com/issues/40174>`_, `pr#29207 <https://github.com/ceph/ceph/pull/29207>`_, Kefu Chai)
* mgr/orchestrator: Cache and DeepSea iSCSI + NFS (`pr#29060 <https://github.com/ceph/ceph/pull/29060>`_, Sebastian Wagner, Tim Serong)
* mgr/rbd_support: support scheduling long-running background operations (`issue#40621 <http://tracker.ceph.com/issues/40621>`_, `issue#40790 <http://tracker.ceph.com/issues/40790>`_, `pr#29725 <https://github.com/ceph/ceph/pull/29725>`_, Venky Shankar, Jason Dillaman)
* mgr: use ipv4 default when ipv6 was disabled (`issue#40023 <http://tracker.ceph.com/issues/40023>`_, `pr#29194 <https://github.com/ceph/ceph/pull/29194>`_, kungf)
* mgr/volumes: background purge queue for subvolumes (`issue#40036 <http://tracker.ceph.com/issues/40036>`_, `pr#29079 <https://github.com/ceph/ceph/pull/29079>`_, Patrick Donnelly, Venky Shankar, Kefu Chai)
* mgr/volumes: minor enhancement and bug fix (`issue#40927 <http://tracker.ceph.com/issues/40927>`_, `issue#40617 <http://tracker.ceph.com/issues/40617>`_, `pr#29490 <https://github.com/ceph/ceph/pull/29490>`_, Ramana Raja)
* mon: auth mon isn't loading full KeyServerData after restart (`issue#40634 <http://tracker.ceph.com/issues/40634>`_, `pr#28993 <https://github.com/ceph/ceph/pull/28993>`_, Sage Weil)
* mon/MgrMonitor: fix null deref when invalid formatter is specified (`pr#29566 <https://github.com/ceph/ceph/pull/29566>`_, Sage Weil)
* mon/OSDMonitor: allow pg_num to increase when require_osd_release < N (`issue#39570 <http://tracker.ceph.com/issues/39570>`_, `pr#29671 <https://github.com/ceph/ceph/pull/29671>`_, Neha Ojha, Sage Weil)
* mon/OSDMonitor.cc: better error message about min_size (`pr#29617 <https://github.com/ceph/ceph/pull/29617>`_, Neha Ojha)
* mon: paxos: introduce new reset_pending_committing_finishers for safety (`issue#39484 <http://tracker.ceph.com/issues/39484>`_, `pr#28528 <https://github.com/ceph/ceph/pull/28528>`_, Greg Farnum)
* mon: set recovery priority etc on cephfs metadata pool (`pr#29275 <https://github.com/ceph/ceph/pull/29275>`_, Sage Weil)
* mon: take the mon lock in handle_conf_change (`issue#39625 <http://tracker.ceph.com/issues/39625>`_, `pr#29373 <https://github.com/ceph/ceph/pull/29373>`_, huangjun)
* msg/async: avoid unnecessary costly wakeups for outbound messages (`pr#29141 <https://github.com/ceph/ceph/pull/29141>`_, Jason Dillaman)
* msg/async: enable secure mode by default, no longer experimental (`pr#29143 <https://github.com/ceph/ceph/pull/29143>`_, Sage Weil)
* msg/async: no-need set connection for Message (`pr#29142 <https://github.com/ceph/ceph/pull/29142>`_, Jianpeng Ma)
* msg/async, v2: make the reset_recv_state() unconditional (`issue#40115 <http://tracker.ceph.com/issues/40115>`_, `pr#29140 <https://github.com/ceph/ceph/pull/29140>`_, Radoslaw Zarzynski, Sage Weil)
* nautilus:common/options.cc: Lower the default value of osd_deep_scrub_large_omap_object_key_threshold (`pr#29173 <https://github.com/ceph/ceph/pull/29173>`_, Neha Ojha)
* osd: Don't randomize deep scrubs when noscrub set (`issue#40198 <http://tracker.ceph.com/issues/40198>`_, `pr#28768 <https://github.com/ceph/ceph/pull/28768>`_, David Zafman)
* osd: Fix the way that auto repair triggers after regular scrub (`issue#40530 <http://tracker.ceph.com/issues/40530>`_, `issue#40073 <http://tracker.ceph.com/issues/40073>`_, `pr#28869 <https://github.com/ceph/ceph/pull/28869>`_, sjust@redhat.com, David Zafman)
* osd/OSD: auto mark heartbeat sessions as stale and tear them down (`issue#40586 <http://tracker.ceph.com/issues/40586>`_, `pr#29391 <https://github.com/ceph/ceph/pull/29391>`_, xie xingguo)
* osd/OSD: keep synchronizing with mon if stuck at booting (`pr#28639 <https://github.com/ceph/ceph/pull/28639>`_, xie xingguo)
* osd/PG: do not queue scrub if PG is not active when unblock (`issue#40451 <http://tracker.ceph.com/issues/40451>`_, `pr#29372 <https://github.com/ceph/ceph/pull/29372>`_, Sage Weil)
* osd/PG: fix cleanup of pgmeta-like objects on PG deletion (`pr#29115 <https://github.com/ceph/ceph/pull/29115>`_, Sage Weil)
* pybind/mgr/rbd_support: ignore missing support for RBD namespaces (`issue#41475 <https://tracker.ceph.com/issues/41475>`_, `pr#29945 <https://github.com/ceph/ceph/pull/29945>`_, Mykola Golub)
* rbd/action: fix error getting positional argument (`issue#40095 <http://tracker.ceph.com/issues/40095>`_, `pr#28870 <https://github.com/ceph/ceph/pull/28870>`_, songweibin)
* rbd: [cli] 'export' should handle concurrent IO completions (`issue#40435 <http://tracker.ceph.com/issues/40435>`_, `pr#29329 <https://github.com/ceph/ceph/pull/29329>`_, Jason Dillaman)
* rbd: librbd: do not unblock IO prior to growing object map during resize (`issue#39952 <http://tracker.ceph.com/issues/39952>`_, `pr#29246 <https://github.com/ceph/ceph/pull/29246>`_, Jason Dillaman)
* rbd-mirror: handle duplicates in image sync throttler queue (`issue#40519 <http://tracker.ceph.com/issues/40519>`_, `pr#28817 <https://github.com/ceph/ceph/pull/28817>`_, Mykola Golub)
* rbd-mirror: link against the specified alloc library (`issue#40110 <http://tracker.ceph.com/issues/40110>`_, `pr#29193 <https://github.com/ceph/ceph/pull/29193>`_, Jason Dillaman)
* rbd-nbd: sscanf return 0 mean not-match (`issue#39269 <http://tracker.ceph.com/issues/39269>`_, `pr#29315 <https://github.com/ceph/ceph/pull/29315>`_, Jianpeng Ma)
* rbd: profile rbd OSD cap should add class rbd metadata_list cap by default (`issue#39973 <http://tracker.ceph.com/issues/39973>`_, `pr#29328 <https://github.com/ceph/ceph/pull/29328>`_, songweibin)
* rbd: Reduce log level for cls/journal and cls/rbd expected errors (`issue#40865 <http://tracker.ceph.com/issues/40865>`_, `pr#29551 <https://github.com/ceph/ceph/pull/29551>`_, Jason Dillaman)
* rbd: tests: add "rbd diff" coverage to suite (`issue#39447 <http://tracker.ceph.com/issues/39447>`_, `pr#28575 <https://github.com/ceph/ceph/pull/28575>`_, Shyukri Shyukriev, Nathan Cutler)
* rgw: add 'GET /admin/realm?list' api to list realms (`issue#39626 <http://tracker.ceph.com/issues/39626>`_, `pr#28751 <https://github.com/ceph/ceph/pull/28751>`_, Casey Bodley)
* rgw: allow radosgw-admin to list bucket w --allow-unordered (`issue#39637 <http://tracker.ceph.com/issues/39637>`_, `pr#28230 <https://github.com/ceph/ceph/pull/28230>`_, J. Eric Ivancich)
* rgw: conditionally allow builtin users with non-unique email addresses (`issue#40089 <http://tracker.ceph.com/issues/40089>`_, `pr#28715 <https://github.com/ceph/ceph/pull/28715>`_, Matt Benjamin)
* rgw: deleting bucket can fail when it contains unfinished multipart uploads (`issue#40526 <http://tracker.ceph.com/issues/40526>`_, `pr#29154 <https://github.com/ceph/ceph/pull/29154>`_, J. Eric Ivancich)
* rgw: Don't crash on copy when metadata directive not supplied (`issue#40416 <http://tracker.ceph.com/issues/40416>`_, `pr#29499 <https://github.com/ceph/ceph/pull/29499>`_, Adam C. Emerson)
* rgw_file: advance_mtime() should consider namespace expiration (`issue#40415 <http://tracker.ceph.com/issues/40415>`_, `pr#29410 <https://github.com/ceph/ceph/pull/29410>`_, Matt Benjamin)
* rgw_file:  advance_mtime() takes RGWFileHandle::mutex unconditionally (`pr#29801 <https://github.com/ceph/ceph/pull/29801>`_, Matt Benjamin)
* rgw_file: all directories are virtual with respect to contents (`issue#40204 <http://tracker.ceph.com/issues/40204>`_, `pr#28886 <https://github.com/ceph/ceph/pull/28886>`_, Matt Benjamin)
* rgw_file:  fix invalidation of top-level directories (`issue#40196 <http://tracker.ceph.com/issues/40196>`_, `pr#29309 <https://github.com/ceph/ceph/pull/29309>`_, Matt Benjamin)
* rgw_file: fix readdir eof() calc--caller stop implies !eof (`issue#40375 <http://tracker.ceph.com/issues/40375>`_, `pr#29409 <https://github.com/ceph/ceph/pull/29409>`_, Matt Benjamin)
* rgw_file: include tenant when hashing bucket names (`issue#40118 <http://tracker.ceph.com/issues/40118>`_, `pr#28854 <https://github.com/ceph/ceph/pull/28854>`_, Matt Benjamin)
* rgw: fix miss get ret in STSService::storeARN (`issue#40386 <http://tracker.ceph.com/issues/40386>`_, `pr#28713 <https://github.com/ceph/ceph/pull/28713>`_, Tianshan Qu)
* rgw: fix prefix handling in LCFilter (`issue#37879 <http://tracker.ceph.com/issues/37879>`_, `pr#28550 <https://github.com/ceph/ceph/pull/28550>`_, Matt Benjamin)
* rgw: fix rgw crash and set correct error code (`pr#28729 <https://github.com/ceph/ceph/pull/28729>`_, yuliyang)
* rgw: hadoop-s3a suite failing with more ansible errors (`issue#39706 <http://tracker.ceph.com/issues/39706>`_, `pr#28735 <https://github.com/ceph/ceph/pull/28735>`_, Casey Bodley)
* rgw: hadoop-s3a suite failing with more ansible errors (`issue#39706 <http://tracker.ceph.com/issues/39706>`_, `pr#29265 <https://github.com/ceph/ceph/pull/29265>`_, Casey Bodley)
* rgw: Librgw doesn't GC deleted object correctly (`issue#37734 <http://tracker.ceph.com/issues/37734>`_, `pr#28648 <https://github.com/ceph/ceph/pull/28648>`_, Tao Chen, Matt Benjamin)
* rgw: multisite: DELETE Bucket CORS is not forwarded to master zone (`issue#39629 <http://tracker.ceph.com/issues/39629>`_, `pr#28714 <https://github.com/ceph/ceph/pull/28714>`_, Chang Liu)
* rgw: multisite: fix --bypass-gc flag for 'radosgw-admin bucket rm' (`issue#24991 <http://tracker.ceph.com/issues/24991>`_, `pr#28549 <https://github.com/ceph/ceph/pull/28549>`_, Casey Bodley)
* rgw: multisite: 'radosgw-admin bilog trim' stops after 1000 entries (`issue#40187 <http://tracker.ceph.com/issues/40187>`_, `pr#29326 <https://github.com/ceph/ceph/pull/29326>`_, Casey Bodley)
* rgw: multisite: 'radosgw-admin bucket sync status' should call syncs_from(source.name) instead of id (`issue#40022 <http://tracker.ceph.com/issues/40022>`_, `pr#28739 <https://github.com/ceph/ceph/pull/28739>`_, Casey Bodley)
* rgw: multisite: radosgw-admin commands should not modify metadata on a non-master zone (`issue#39548 <http://tracker.ceph.com/issues/39548>`_, `pr#29163 <https://github.com/ceph/ceph/pull/29163>`_, Shilpa Jagannath)
* rgw: multisite: RGWListBucketIndexesCR for data full sync needs pagination (`issue#39551 <http://tracker.ceph.com/issues/39551>`_, `pr#29311 <https://github.com/ceph/ceph/pull/29311>`_, Shilpa Jagannath)
* rgw/OutputDataSocket: append_output(buffer::list&) says it will (but does not) discard output at data_max_backlog (`issue#40178 <http://tracker.ceph.com/issues/40178>`_, `pr#29310 <https://github.com/ceph/ceph/pull/29310>`_, Matt Benjamin)
* rgw, Policy should be url_decode when assume_role (`pr#28728 <https://github.com/ceph/ceph/pull/28728>`_, yuliyang)
* rgw: provide admin-friendly reshard status output (`issue#37615 <http://tracker.ceph.com/issues/37615>`_, `pr#29286 <https://github.com/ceph/ceph/pull/29286>`_, Mark Kogan)
* rgw: Put LC doesn't clear existing lifecycle (`issue#39654 <http://tracker.ceph.com/issues/39654>`_, `pr#29313 <https://github.com/ceph/ceph/pull/29313>`_, Abhishek Lekshmanan)
* rgw: remove rgw_num_rados_handles; set autoscale parameters or rgw metadata pools (`pr#27684 <https://github.com/ceph/ceph/pull/27684>`_, Adam C. Emerson, Casey Bodley, Sage Weil)
* rgw: RGWGC add perfcounter retire counter (`issue#38251 <http://tracker.ceph.com/issues/38251>`_, `pr#29308 <https://github.com/ceph/ceph/pull/29308>`_, Matt Benjamin)
* rgw: Save an unnecessary copy of RGWEnv (`issue#40183 <http://tracker.ceph.com/issues/40183>`_, `pr#29205 <https://github.com/ceph/ceph/pull/29205>`_, Mark Kogan)
* rgw: set null version object issues (`issue#36763 <http://tracker.ceph.com/issues/36763>`_, `pr#29287 <https://github.com/ceph/ceph/pull/29287>`_, Tianshan Qu)
* rgw: Swift interface: server side copy fails if object name contains "?" (`issue#27217 <http://tracker.ceph.com/issues/27217>`_, `pr#28736 <https://github.com/ceph/ceph/pull/28736>`_, Casey Bodley)
* rgw: TempURL should not allow PUTs with the X-Object-Manifest (`issue#20797 <http://tracker.ceph.com/issues/20797>`_, `pr#28712 <https://github.com/ceph/ceph/pull/28712>`_, Radoslaw Zarzynski)
* rgw: the Multi-Object Delete operation of S3 API wrongly handles the Code response element (`issue#18241 <http://tracker.ceph.com/issues/18241>`_, `pr#28737 <https://github.com/ceph/ceph/pull/28737>`_, Radoslaw Zarzynski)
* rocksdb: rocksdb_rmrange related improvements (`pr#29439 <https://github.com/ceph/ceph/pull/29439>`_, Zengran Zhang, Sage Weil)
* rocksdb: Updated to v6.1.2 (`pr#29440 <https://github.com/ceph/ceph/pull/29440>`_, Mark Nelson)
* tools: ceph-kvstore-tool: print db stats (`pr#28810 <https://github.com/ceph/ceph/pull/28810>`_, Igor Fedotov)


v14.2.2 Nautilus
================

This is the second bug fix release of Ceph Nautilus release series. We recommend
all Nautilus users upgrade to this release. For upgrading from older releases of
ceph, general guidelines for upgrade to nautilus must be followed
:ref:`nautilus-old-upgrade`.

Notable Changes
---------------

* The no{up,down,in,out} related commands have been revamped.
  There are now 2 ways to set the no{up,down,in,out} flags:
  the old 'ceph osd [un]set <flag>' command, which sets cluster-wide flags;
  and the new 'ceph osd [un]set-group <flags> <who>' command,
  which sets flags in batch at the granularity of any crush node,
  or device class.

* radosgw-admin introduces two subcommands that allow the
  managing of expire-stale objects that might be left behind after a
  bucket reshard in earlier versions of RGW. One subcommand lists such
  objects and the other deletes them. Read the troubleshooting section
  of the dynamic resharding docs for details.

* Earlier Nautilus releases (14.2.1 and 14.2.0) have an issue where
  deploying a single new (Nautilus) BlueStore OSD on an upgraded
  cluster (i.e. one that was originally deployed pre-Nautilus) breaks
  the pool utilization stats reported by ``ceph df``.  Until all OSDs
  have been reprovisioned or updated (via ``ceph-bluestore-tool
  repair``), the pool stats will show values that are lower than the
  true value.  This is resolved in 14.2.2, such that the cluster only
  switches to using the more accurate per-pool stats after *all* OSDs
  are 14.2.2 (or later), are BlueStore, and (if they were created
  prior to Nautilus) have been updated via the ``repair`` function.

* The default value for `mon_crush_min_required_version` has been
  changed from `firefly` to `hammer`, which means the cluster will
  issue a health warning if your CRUSH tunables are older than hammer.
  There is generally a small (but non-zero) amount of data that will
  move around by making the switch to hammer tunables; for more information,
  see :ref:`crush-map-tunables`.

  If possible, we recommend that you set the oldest allowed client to `hammer`
  or later.  You can tell what the current oldest allowed client is with::

    ceph osd dump | grep min_compat_client

  If the current value is older than hammer, you can tell whether it
  is safe to make this change by verifying that there are no clients
  older than hammer current connected to the cluster with::

    ceph features

  The newer `straw2` CRUSH bucket type was introduced in hammer, and
  ensuring that all clients are hammer or newer allows new features
  only supported for `straw2` buckets to be used, including the
  `crush-compat` mode for the :ref:`balancer`.

Changelog
---------

* bluestore: backport more bluestore alerts (`pr#27645 <https://github.com/ceph/ceph/pull/27645>`_, Sage Weil, Igor Fedotov)
* bluestore: call fault_range prior to looking for blob to reuse (`pr#27525 <https://github.com/ceph/ceph/pull/27525>`_, Igor Fedotov)
* bluestore: correctly measure deferred writes into new blobs (`issue#38816 <http://tracker.ceph.com/issues/38816>`_, `pr#27819 <https://github.com/ceph/ceph/pull/27819>`_, Sage Weil)
* bluestore: dump before "no-spanning blob id" abort (`pr#28028 <https://github.com/ceph/ceph/pull/28028>`_, Igor Fedotov)
* bluestore: fix for FreeBSD iocb structure (`issue#39612 <http://tracker.ceph.com/issues/39612>`_, `pr#28007 <https://github.com/ceph/ceph/pull/28007>`_, Willem Jan Withagen)
* bluestore: fix missing discard in BlueStore::_kv_sync_thread (`issue#39672 <http://tracker.ceph.com/issues/39672>`_, `pr#28258 <https://github.com/ceph/ceph/pull/28258>`_, Junhui Tang)
* bluestore: fix out-of-bound access in bmap allocator (`pr#27740 <https://github.com/ceph/ceph/pull/27740>`_, Igor Fedotov)
* bluestore: fix duplicate allocations in bmap allocator (`issue#40080 <http://tracker.ceph.com/issues/40080>`_, `pr#28646 <https://github.com/ceph/ceph/pull/28646>`_, Igor Fedotov)
* build/ops: Ceph RPM build fails on openSUSE Tumbleweed with GCC 9 (`issue#40067 <http://tracker.ceph.com/issues/40067>`_, `issue#39974 <http://tracker.ceph.com/issues/39974>`_, `pr#28299 <https://github.com/ceph/ceph/pull/28299>`_, Martin Liška)
* build/ops: cmake: Fix build against ncurses with separate libtinfo (`pr#27532 <https://github.com/ceph/ceph/pull/27532>`_, Lars Wendler)
* build/ops: cmake: set empty-string RPATH for ceph-osd (`issue#40301 <http://tracker.ceph.com/issues/40301>`_, `issue#40295 <http://tracker.ceph.com/issues/40295>`_, `pr#28516 <https://github.com/ceph/ceph/pull/28516>`_, Nathan Cutler)
* build/ops: do_cmake.sh: source not found (`issue#39981 <http://tracker.ceph.com/issues/39981>`_, `issue#40003 <http://tracker.ceph.com/issues/40003>`_, `pr#28215 <https://github.com/ceph/ceph/pull/28215>`_, Nathan Cutler)
* build/ops: python3 pybind RPMs do not replace their python2 counterparts on upgrade even though they should (`issue#40099 <http://tracker.ceph.com/issues/40099>`_, `issue#40232 <http://tracker.ceph.com/issues/40232>`_, `pr#28469 <https://github.com/ceph/ceph/pull/28469>`_, Nathan Cutler)
* build/ops: rpm: install grafana dashboards world readable (`pr#28392 <https://github.com/ceph/ceph/pull/28392>`_, Jan Fajerski)
* build/ops: selinux: Update the policy for RHEL8 (`pr#28511 <https://github.com/ceph/ceph/pull/28511>`_, Boris Ranto)
* ceph-volume: add utility functions (`pr#27791 <https://github.com/ceph/ceph/pull/27791>`_, Mohamad Gebai)
* ceph-volume: broken assertion errors after pytest changes (`pr#28925 <https://github.com/ceph/ceph/pull/28925>`_, Alfredo Deza)
* ceph-volume: look for rotational data in lsblk (`pr#27723 <https://github.com/ceph/ceph/pull/27723>`_, Andrew Schoen)
* ceph-volume: tests add a sleep in tox for slow OSDs after booting (`pr#28924 <https://github.com/ceph/ceph/pull/28924>`_, Alfredo Deza)
* ceph-volume: use the Device.rotational property instead of sys_api (`pr#29028 <https://github.com/ceph/ceph/pull/29028>`_, Andrew Schoen)
* cephfs-shell: Revert "cephfs.pyx: add py3 compatibility (`pr#28641 <https://github.com/ceph/ceph/pull/28641>`_, Varsha Rao)
* cephfs-shell: ls command produces error: no colorize attribute found error (`issue#39376 <http://tracker.ceph.com/issues/39376>`_, `issue#39378 <http://tracker.ceph.com/issues/39378>`_, `issue#38740 <http://tracker.ceph.com/issues/38740>`_, `issue#39379 <http://tracker.ceph.com/issues/39379>`_, `issue#39197 <http://tracker.ceph.com/issues/39197>`_, `issue#39377 <http://tracker.ceph.com/issues/39377>`_, `pr#27677 <https://github.com/ceph/ceph/pull/27677>`_, Milind Changire, Varsha Rao)
* cephfs-shell: misc. cephfs-shell backports (`issue#40314 <http://tracker.ceph.com/issues/40314>`_, `issue#40471 <http://tracker.ceph.com/issues/40471>`_, `issue#40418 <http://tracker.ceph.com/issues/40418>`_, `issue#40469 <http://tracker.ceph.com/issues/40469>`_, `issue#40313 <http://tracker.ceph.com/issues/40313>`_, `issue#39937 <http://tracker.ceph.com/issues/39937>`_, `issue#39678 <http://tracker.ceph.com/issues/39678>`_, `issue#40244 <http://tracker.ceph.com/issues/40244>`_, `issue#39404 <http://tracker.ceph.com/issues/39404>`_, `issue#40243 <http://tracker.ceph.com/issues/40243>`_, `issue#39165 <http://tracker.ceph.com/issues/39165>`_, `issue#40470 <http://tracker.ceph.com/issues/40470>`_, `issue#40455 <http://tracker.ceph.com/issues/40455>`_, `issue#39936 <http://tracker.ceph.com/issues/39936>`_, `issue#40217 <http://tracker.ceph.com/issues/40217>`_, `pr#28681 <https://github.com/ceph/ceph/pull/28681>`_, Patrick Donnelly, Varsha Rao, Milind Changire)
* cephfs-shell: mkdir error for relative path (`issue#39960 <http://tracker.ceph.com/issues/39960>`_, `pr#28616 <https://github.com/ceph/ceph/pull/28616>`_, Varsha Rao)
* cephfs: FSAL_CEPH assertion failed in Client::_lookup_name: "parent->is_dir() (`issue#40085 <http://tracker.ceph.com/issues/40085>`_, `issue#40161 <http://tracker.ceph.com/issues/40161>`_, `pr#28612 <https://github.com/ceph/ceph/pull/28612>`_, Jeff Layton)
* cephfs: ceph_volume_client: Too many arguments for "WriteOpCtx (`issue#39050 <http://tracker.ceph.com/issues/39050>`_, `issue#38946 <http://tracker.ceph.com/issues/38946>`_, `pr#27893 <https://github.com/ceph/ceph/pull/27893>`_, Ramana Raja)
* cephfs: client: ceph.dir.rctime xattr value incorrectly prefixes 09 to the nanoseconds component (`issue#40167 <http://tracker.ceph.com/issues/40167>`_, `pr#28500 <https://github.com/ceph/ceph/pull/28500>`_, David Disseldorp)
* cephfs: client: fix "ceph.snap.btime" vxattr value (`issue#40169 <http://tracker.ceph.com/issues/40169>`_, `pr#28499 <https://github.com/ceph/ceph/pull/28499>`_, David Disseldorp)
* cephfs: client: fix fuse client hang because its bad session PipeConnection (`issue#39686 <http://tracker.ceph.com/issues/39686>`_, `issue#39305 <http://tracker.ceph.com/issues/39305>`_, `pr#28375 <https://github.com/ceph/ceph/pull/28375>`_, Guan yunfei)
* cephfs: kclient: nofail option not supported (`issue#39232 <http://tracker.ceph.com/issues/39232>`_, `pr#27851 <https://github.com/ceph/ceph/pull/27851>`_, Kenneth Waegeman)
* cephfs: mds: Expose CephFS snapshot creation time to clients (`issue#39471 <http://tracker.ceph.com/issues/39471>`_, `pr#27901 <https://github.com/ceph/ceph/pull/27901>`_, David Disseldorp)
* cephfs: mds: MDSTableServer.cc: 83: FAILED assert(version == tid) (`issue#39211 <http://tracker.ceph.com/issues/39211>`_, `issue#38835 <http://tracker.ceph.com/issues/38835>`_, `pr#27853 <https://github.com/ceph/ceph/pull/27853>`_, "Yan, Zheng")
* cephfs: mds: avoid sending too many osd requests at once after mds restarts (`issue#40028 <http://tracker.ceph.com/issues/40028>`_, `issue#40040 <http://tracker.ceph.com/issues/40040>`_, `pr#28582 <https://github.com/ceph/ceph/pull/28582>`_, simon gao)
* cephfs: mds: behind on trimming and "[dentry] was purgeable but no longer is! (`issue#39222 <http://tracker.ceph.com/issues/39222>`_, `issue#38679 <http://tracker.ceph.com/issues/38679>`_, `pr#27879 <https://github.com/ceph/ceph/pull/27879>`_, "Yan, Zheng")
* cephfs: mds: better output of 'ceph health detail (`issue#39266 <http://tracker.ceph.com/issues/39266>`_, `pr#27846 <https://github.com/ceph/ceph/pull/27846>`_, Shen Hang')
* cephfs: mds: check dir fragment to split dir if mkdir makes it oversized (`issue#39690 <http://tracker.ceph.com/issues/39690>`_, `pr#28394 <https://github.com/ceph/ceph/pull/28394>`_, Erqi Chen)
* cephfs: mds: check directory split after rename (`issue#39199 <http://tracker.ceph.com/issues/39199>`_, `issue#38994 <http://tracker.ceph.com/issues/38994>`_, `pr#27736 <https://github.com/ceph/ceph/pull/27736>`_, Shen Hang)
* cephfs: mds: drop reconnect message from non-existent session (`issue#39026 <http://tracker.ceph.com/issues/39026>`_, `issue#39192 <http://tracker.ceph.com/issues/39192>`_, `pr#27714 <https://github.com/ceph/ceph/pull/27714>`_, Shen Hang)
* cephfs: mds: fail to resolve snapshot name contains '_' (`issue#39473 <http://tracker.ceph.com/issues/39473>`_, `pr#27849 <https://github.com/ceph/ceph/pull/27849>`_, "Yan, Zheng')
* cephfs: mds: fix 'is session in blacklist' check in Server::apply_blacklist() (`issue#40236 <http://tracker.ceph.com/issues/40236>`_, `issue#40061 <http://tracker.ceph.com/issues/40061>`_, `pr#28618 <https://github.com/ceph/ceph/pull/28618>`_, "Yan, Zheng')
* cephfs: mds: fix corner case of replaying open sessions (`pr#28580 <https://github.com/ceph/ceph/pull/28580>`_, "Yan, Zheng")
* cephfs: mds: high debug logging with many subtrees is slow (`issue#38876 <http://tracker.ceph.com/issues/38876>`_, `pr#27892 <https://github.com/ceph/ceph/pull/27892>`_, Rishabh Dave)
* cephfs: mds: initialize cap_revoke_eviction_timeout with conf (`issue#39209 <http://tracker.ceph.com/issues/39209>`_, `issue#38844 <http://tracker.ceph.com/issues/38844>`_, `pr#27842 <https://github.com/ceph/ceph/pull/27842>`_, simon gao)
* cephfs: mds: output lock state in format dump (`issue#39645 <http://tracker.ceph.com/issues/39645>`_, `issue#39670 <http://tracker.ceph.com/issues/39670>`_, `pr#28233 <https://github.com/ceph/ceph/pull/28233>`_, Zhi Zhang)
* cephfs: mds: reset heartbeat during long-running loops in recovery (`issue#40223 <http://tracker.ceph.com/issues/40223>`_, `pr#28611 <https://github.com/ceph/ceph/pull/28611>`_, "Yan, Zheng")
* cephfs: mds: there is an assertion when calling Beacon::shutdown() (`issue#39214 <http://tracker.ceph.com/issues/39214>`_, `issue#38822 <http://tracker.ceph.com/issues/38822>`_, `pr#27852 <https://github.com/ceph/ceph/pull/27852>`_, huanwen ren)
* cephfs: mount: key parsing fail when doing a remount (`issue#40164 <http://tracker.ceph.com/issues/40164>`_, `pr#28610 <https://github.com/ceph/ceph/pull/28610>`_, Luis Henriques)
* cephfs: pybind: added lseek() (`pr#28333 <https://github.com/ceph/ceph/pull/28333>`_, Xiaowei Chu)
* common/assert: include ceph_abort_msg(arg) arg in log output (`pr#27824 <https://github.com/ceph/ceph/pull/27824>`_, Sage Weil)
* common/options: annotate some options; enable some runtime updates (`pr#27818 <https://github.com/ceph/ceph/pull/27818>`_, Sage Weil)
* common/options: update mon_crush_min_required_version=hammer (`pr#27625 <https://github.com/ceph/ceph/pull/27625>`_, Sage Weil)
* common/util: handle long lines in /proc/cpuinfo (`issue#38296 <http://tracker.ceph.com/issues/38296>`_, `issue#39476 <http://tracker.ceph.com/issues/39476>`_, `pr#28141 <https://github.com/ceph/ceph/pull/28141>`_, Sage Weil)
* common: Clang requires a default constructor, but it can be empty (`issue#39561 <http://tracker.ceph.com/issues/39561>`_, `issue#39573 <http://tracker.ceph.com/issues/39573>`_, `pr#28131 <https://github.com/ceph/ceph/pull/28131>`_, Willem Jan Withagen)
* common: fix parse_env nullptr deref (`pr#28382 <https://github.com/ceph/ceph/pull/28382>`_, Patrick Donnelly)
* common: make cluster_network work (`issue#39671 <http://tracker.ceph.com/issues/39671>`_, `pr#28248 <https://github.com/ceph/ceph/pull/28248>`_, Jianpeng Ma)
* common: parse ISO 8601 datetime format (`issue#40087 <http://tracker.ceph.com/issues/40087>`_, `pr#28325 <https://github.com/ceph/ceph/pull/28325>`_, Sage Weil)
* core: Give recovery for inactive PGs a higher priority (`issue#39504 <http://tracker.ceph.com/issues/39504>`_, `issue#38195 <http://tracker.ceph.com/issues/38195>`_, `pr#27854 <https://github.com/ceph/ceph/pull/27854>`_, David Zafman)
* core: mon,osd: add no{out,down,in,out} flags on CRUSH nodes (`pr#27623 <https://github.com/ceph/ceph/pull/27623>`_, xie xingguo, Sage Weil)
* core: mon/Elector: format mon_release correctly (`issue#39419 <http://tracker.ceph.com/issues/39419>`_, `pr#27771 <https://github.com/ceph/ceph/pull/27771>`_, Sage Weil)
* core: mon/Monitor: allow probe if MMonProbe::mon_release == 0 (`issue#38850 <http://tracker.ceph.com/issues/38850>`_, `pr#28262 <https://github.com/ceph/ceph/pull/28262>`_, Sage Weil)
* core: mon: fix off-by-one rendering progress bar (`pr#28398 <https://github.com/ceph/ceph/pull/28398>`_, Sage Weil)
* core: mon: use per-pool stats only when all OSDs are reporting (`pr#29032 <https://github.com/ceph/ceph/pull/29032>`_, Sage Weil)
* core: monitoring: Provide a base set of Prometheus alert manager rules that notify the user about common Ceph error conditions (`issue#39540 <http://tracker.ceph.com/issues/39540>`_, `pr#27998 <https://github.com/ceph/ceph/pull/27998>`_, Jan Fajerski)
* core: monitoring: update Grafana dashboards (`issue#39652 <http://tracker.ceph.com/issues/39652>`_, `issue#40006 <http://tracker.ceph.com/issues/40006>`_, `issue#39971 <http://tracker.ceph.com/issues/39971>`_, `issue#39932 <http://tracker.ceph.com/issues/39932>`_, `pr#28101 <https://github.com/ceph/ceph/pull/28101>`_, Kiefer Chang, Jan Fajerski)
* core: osd/OSD.cc: make osd bench description consistent with parameters (`issue#39006 <http://tracker.ceph.com/issues/39006>`_, `issue#39375 <http://tracker.ceph.com/issues/39375>`_, `pr#28035 <https://github.com/ceph/ceph/pull/28035>`_, Neha Ojha)
* core: osd/OSDMap: Replace get_out_osds with get_out_existing_osds (`issue#39421 <http://tracker.ceph.com/issues/39421>`_, `issue#39154 <http://tracker.ceph.com/issues/39154>`_, `pr#28072 <https://github.com/ceph/ceph/pull/28072>`_, Brad Hubbard)
* core: osd/PG: discover missing objects when an OSD peers and PG is degraded (`pr#27744 <https://github.com/ceph/ceph/pull/27744>`_, Jonas Jelten)
* core: osd/PG: do not use approx_missing_objects pre-nautilus (`issue#39512 <http://tracker.ceph.com/issues/39512>`_, `pr#28160 <https://github.com/ceph/ceph/pull/28160>`_, Neha Ojha)
* core: osd/PG: fix last_complete re-calculation on splitting (`issue#39539 <http://tracker.ceph.com/issues/39539>`_, `issue#26958 <http://tracker.ceph.com/issues/26958>`_, `pr#28219 <https://github.com/ceph/ceph/pull/28219>`_, xie xingguo)
* core: osd/PG: skip rollforward when !transaction_applied during append_log() (`issue#36739 <http://tracker.ceph.com/issues/36739>`_, `issue#38881 <http://tracker.ceph.com/issues/38881>`_, `pr#27654 <https://github.com/ceph/ceph/pull/27654>`_, Neha Ojha)
* core: osd/PGLog: preserve original_crt to check rollbackability (`issue#36739 <http://tracker.ceph.com/issues/36739>`_, `issue#39043 <http://tracker.ceph.com/issues/39043>`_, `pr#27632 <https://github.com/ceph/ceph/pull/27632>`_, Neha Ojha)
* core: osd: Don't evict after a flush if intersecting scrub range (`issue#38840 <http://tracker.ceph.com/issues/38840>`_, `issue#39519 <http://tracker.ceph.com/issues/39519>`_, `pr#28205 <https://github.com/ceph/ceph/pull/28205>`_, David Zafman')
* core: osd: Don't include user changeable flag in snaptrim related assert (`issue#39699 <http://tracker.ceph.com/issues/39699>`_, `issue#38124 <http://tracker.ceph.com/issues/38124>`_, `pr#28203 <https://github.com/ceph/ceph/pull/28203>`_, David Zafman')
* core: osd: FAILED ceph_assert(attrs || !pg_log.get_missing().is_missing(soid) || (it_objects != pg_log.get_log().objects.end() && it_objects->second->op == pg_log_entry_t::LOST_REVERT)) in PrimaryLogPG::get_object_context() (`issue#38931 <http://tracker.ceph.com/issues/38931>`_, `issue#39219 <http://tracker.ceph.com/issues/39219>`_, `issue#38784 <http://tracker.ceph.com/issues/38784>`_, `pr#27839 <https://github.com/ceph/ceph/pull/27839>`_, xie xingguo)
* core: osd: Include dups in copy_after() and copy_up_to() (`issue#39304 <http://tracker.ceph.com/issues/39304>`_, `pr#28088 <https://github.com/ceph/ceph/pull/28088>`_, David Zafman)
* core: osd: Increase log level of messages which unnecessarily fill up logs (`pr#27687 <https://github.com/ceph/ceph/pull/27687>`_, David Zafman)
* core: osd: Output Base64 encoding of CRC header if binary data present (`issue#39738 <http://tracker.ceph.com/issues/39738>`_, `pr#28504 <https://github.com/ceph/ceph/pull/28504>`_, David Zafman)
* core: osd: Primary won't automatically repair replica on pulling error (`issue#39101 <http://tracker.ceph.com/issues/39101>`_, `issue#39184 <http://tracker.ceph.com/issues/39184>`_, `pr#27711 <https://github.com/ceph/ceph/pull/27711>`_, xie xingguo, David Zafman')
* core: osd: revamp {noup,nodown,noin,noout} related commands (`pr#28400 <https://github.com/ceph/ceph/pull/28400>`_, xie xingguo)
* core: osd: shutdown recovery_request_timer earlier (`issue#39205 <http://tracker.ceph.com/issues/39205>`_, `pr#27803 <https://github.com/ceph/ceph/pull/27803>`_, Zengran Zhang)
* core: osd: take heartbeat_lock when calling heartbeat() (`issue#39514 <http://tracker.ceph.com/issues/39514>`_, `issue#39439 <http://tracker.ceph.com/issues/39439>`_, `pr#28164 <https://github.com/ceph/ceph/pull/28164>`_, Sage Weil)
* doc: add LAZYIO (`issue#39051 <http://tracker.ceph.com/issues/39051>`_, `issue#38729 <http://tracker.ceph.com/issues/38729>`_, `pr#27899 <https://github.com/ceph/ceph/pull/27899>`_, "Yan, Zheng")
* doc: add documentation for "fs set min_compat_client" (`issue#39130 <http://tracker.ceph.com/issues/39130>`_, `issue#39176 <http://tracker.ceph.com/issues/39176>`_, `pr#27900 <https://github.com/ceph/ceph/pull/27900>`_, Patrick Donnelly)
* doc: cleanup HTTP Frontends documentation (`issue#38874 <http://tracker.ceph.com/issues/38874>`_, `pr#27922 <https://github.com/ceph/ceph/pull/27922>`_, Casey Bodley)
* doc: dashboard documentation changes (`pr#27642 <https://github.com/ceph/ceph/pull/27642>`_, Tatjana Dehler, Lenz Grimmer)
* doc: orchestrator_cli: Rook orch supports mon update (`issue#39169 <http://tracker.ceph.com/issues/39169>`_, `issue#39137 <http://tracker.ceph.com/issues/39137>`_, `pr#27488 <https://github.com/ceph/ceph/pull/27488>`_, Sebastian Wagner)
* doc: osd_internals/async_recovery: update cost calculation (`pr#28046 <https://github.com/ceph/ceph/pull/28046>`_, Neha Ojha)
* doc: rados/operations/devices: document device prediction (`pr#27752 <https://github.com/ceph/ceph/pull/27752>`_, Sage Weil)
* mgr/ActivePyModules: handle_command - fix broken lock (`issue#39235 <http://tracker.ceph.com/issues/39235>`_, `issue#39308 <http://tracker.ceph.com/issues/39308>`_, `pr#27939 <https://github.com/ceph/ceph/pull/27939>`_, xie xingguo)
* mgr/BaseMgrModule: run MonCommandCompletion on the finisher (`issue#39397 <http://tracker.ceph.com/issues/39397>`_, `issue#39335 <http://tracker.ceph.com/issues/39335>`_, `pr#27699 <https://github.com/ceph/ceph/pull/27699>`_, Sage Weil)
* mgr/ansible: Host ls implementation (`issue#39559 <http://tracker.ceph.com/issues/39559>`_, `pr#27919 <https://github.com/ceph/ceph/pull/27919>`_, Juan Miguel Olmo Mart\xc3\xadnez)
* mgr/balancer: various compat weight-set fixes (`pr#28279 <https://github.com/ceph/ceph/pull/28279>`_, xie xingguo)
* mgr/dashboard: Add custom dialogue for configuring PG scrub parameters (`issue#40059 <http://tracker.ceph.com/issues/40059>`_, `pr#28555 <https://github.com/ceph/ceph/pull/28555>`_, Tatjana Dehler)
* mgr/dashboard: Admin resource not honored (`issue#39338 <http://tracker.ceph.com/issues/39338>`_, `issue#39467 <http://tracker.ceph.com/issues/39467>`_, `pr#27868 <https://github.com/ceph/ceph/pull/27868>`_, Wido den Hollander)
* mgr/dashboard: Angular is creating multiple instances of the same service (`issue#39996 <http://tracker.ceph.com/issues/39996>`_, `issue#40075 <http://tracker.ceph.com/issues/40075>`_, `pr#28312 <https://github.com/ceph/ceph/pull/28312>`_, Tiago Melo)
* mgr/dashboard: Avoid merge conflicts in messages.xlf by auto-generating it at build time? (`issue#39658 <http://tracker.ceph.com/issues/39658>`_, `pr#28178 <https://github.com/ceph/ceph/pull/28178>`_, Sebastian Krah)
* mgr/dashboard: Display correct dialog title (`pr#28189 <https://github.com/ceph/ceph/pull/28189>`_, Volker Theile)
* mgr/dashboard: Error creating NFS client without squash (`issue#40074 <http://tracker.ceph.com/issues/40074>`_, `pr#28311 <https://github.com/ceph/ceph/pull/28311>`_, Tiago Melo)
* mgr/dashboard: KV-table transforms dates through pipe (`issue#39558 <http://tracker.ceph.com/issues/39558>`_, `pr#28021 <https://github.com/ceph/ceph/pull/28021>`_, Stephan M\xc3\xbcller)
* mgr/dashboard: Localization for date picker module (`issue#39371 <http://tracker.ceph.com/issues/39371>`_, `pr#27673 <https://github.com/ceph/ceph/pull/27673>`_, Stephan M\xc3\xbcller)
* mgr/dashboard: Manager should complain about wrong dashboard certificate (`issue#39346 <http://tracker.ceph.com/issues/39346>`_, `pr#27742 <https://github.com/ceph/ceph/pull/27742>`_, Volker Theile)
* mgr/dashboard: NFS clients information is not displayed in the details view (`issue#40057 <http://tracker.ceph.com/issues/40057>`_, `pr#28318 <https://github.com/ceph/ceph/pull/28318>`_, Tiago Melo)
* mgr/dashboard: NFS export creation: Add more info to the validation message of the field Pseudo (`issue#39975 <http://tracker.ceph.com/issues/39975>`_, `issue#39327 <http://tracker.ceph.com/issues/39327>`_, `pr#28320 <https://github.com/ceph/ceph/pull/28320>`_, Tiago Melo)
* mgr/dashboard: Only one root node is shown in the crush map viewer (`issue#39647 <http://tracker.ceph.com/issues/39647>`_, `issue#40077 <http://tracker.ceph.com/issues/40077>`_, `pr#28316 <https://github.com/ceph/ceph/pull/28316>`_, Tiago Melo)
* mgr/dashboard: Push Grafana dashboards on startup (`pr#28635 <https://github.com/ceph/ceph/pull/28635>`_, Zack Cerza)
* mgr/dashboard: Queue notifications as default (`issue#39560 <http://tracker.ceph.com/issues/39560>`_, `pr#28022 <https://github.com/ceph/ceph/pull/28022>`_, Stephan M\xc3\xbcller)
* mgr/dashboard: RBD snapshot name suggestion with local time suffix (`issue#39534 <http://tracker.ceph.com/issues/39534>`_, `pr#27890 <https://github.com/ceph/ceph/pull/27890>`_, Stephan M\xc3\xbcller)
* mgr/dashboard: Reduce the number of renders on the tables (`issue#39944 <http://tracker.ceph.com/issues/39944>`_, `issue#40076 <http://tracker.ceph.com/issues/40076>`_, `pr#28315 <https://github.com/ceph/ceph/pull/28315>`_, Tiago Melo)
* mgr/dashboard: Some validations are not updated and prevent the submission of a form (`issue#40030 <http://tracker.ceph.com/issues/40030>`_, `pr#28319 <https://github.com/ceph/ceph/pull/28319>`_, Tiago Melo)
* mgr/dashboard: Unable to see tcmu-runner perf counters (`issue#39988 <http://tracker.ceph.com/issues/39988>`_, `pr#28191 <https://github.com/ceph/ceph/pull/28191>`_, Ricardo Marques)
* mgr/dashboard: Unify the look of dashboard charts (`issue#39384 <http://tracker.ceph.com/issues/39384>`_, `issue#39961 <http://tracker.ceph.com/issues/39961>`_, `pr#28175 <https://github.com/ceph/ceph/pull/28175>`_, Tiago Melo)
* mgr/dashboard: Validate if any client belongs to more than one group (`issue#39036 <http://tracker.ceph.com/issues/39036>`_, `issue#39454 <http://tracker.ceph.com/issues/39454>`_, `pr#27760 <https://github.com/ceph/ceph/pull/27760>`_, Tiago Melo)
* mgr/dashboard: code documentation (`issue#39345 <http://tracker.ceph.com/issues/39345>`_, `issue#36243 <http://tracker.ceph.com/issues/36243>`_, `pr#27746 <https://github.com/ceph/ceph/pull/27746>`_, Ernesto Puerta)
* mgr/dashboard: iSCSI GET requests should not be logged (`pr#28024 <https://github.com/ceph/ceph/pull/28024>`_, Ricardo Marques)
* mgr/dashboard: iSCSI form does not support IPv6 (`pr#28026 <https://github.com/ceph/ceph/pull/28026>`_, Ricardo Marques)
* mgr/dashboard: iSCSI form is showing a warning (`issue#39452 <http://tracker.ceph.com/issues/39452>`_, `issue#39324 <http://tracker.ceph.com/issues/39324>`_, `pr#27758 <https://github.com/ceph/ceph/pull/27758>`_, Tiago Melo)
* mgr/dashboard: iSCSI should allow exporting an RBD image with Journaling enabled (`pr#28011 <https://github.com/ceph/ceph/pull/28011>`_, Ricardo Marques)
* mgr/dashboard: inconsistent result when editing a RBD image's features (`issue#39993 <http://tracker.ceph.com/issues/39993>`_, `issue#39933 <http://tracker.ceph.com/issues/39933>`_, `pr#28218 <https://github.com/ceph/ceph/pull/28218>`_, Kiefer Chang')
* mgr/dashboard: incorrect help message for minimum blob size (`issue#39624 <http://tracker.ceph.com/issues/39624>`_, `issue#39664 <http://tracker.ceph.com/issues/39664>`_, `pr#28062 <https://github.com/ceph/ceph/pull/28062>`_, Kiefer Chang)
* mgr/dashboard: local variable 'cluster_id' referenced before assignment error when trying to list NFS Ganesha daemons (`issue#40031 <http://tracker.ceph.com/issues/40031>`_, `pr#28261 <https://github.com/ceph/ceph/pull/28261>`_, Nur Faizin')
* mgr/dashboard: make auth token work with UTC times only (`issue#39524 <http://tracker.ceph.com/issues/39524>`_, `issue#39300 <http://tracker.ceph.com/issues/39300>`_, `pr#27942 <https://github.com/ceph/ceph/pull/27942>`_, Ricardo Dias)
* mgr/dashboard: openssl exception when verifying certificates of HTTPS requests (`issue#39962 <http://tracker.ceph.com/issues/39962>`_, `issue#39628 <http://tracker.ceph.com/issues/39628>`_, `pr#28163 <https://github.com/ceph/ceph/pull/28163>`_, Ricardo Dias)
* mgr/dashboard: orchestrator mgr modules assert failure on iscsi service request (`issue#40037 <http://tracker.ceph.com/issues/40037>`_, `pr#28552 <https://github.com/ceph/ceph/pull/28552>`_, Sebastian Wagner)
* mgr/dashboard: show degraded/misplaced/unfound objects (`pr#28584 <https://github.com/ceph/ceph/pull/28584>`_, Alfonso Mart\xc3\xadnez)
* mgr/orchestrator: Remove "(add|test|remove)_stateful_service_rule (`issue#38808 <http://tracker.ceph.com/issues/38808>`_, `pr#27043 <https://github.com/ceph/ceph/pull/27043>`_, Sebastian Wagner)
* mgr/orchestrator: add progress events to all orchestrators (`pr#28040 <https://github.com/ceph/ceph/pull/28040>`_, Sebastian Wagner)
* mgr/progress: behave if pgs disappear (due to a racing pg merge) (`issue#38157 <http://tracker.ceph.com/issues/38157>`_, `issue#39344 <http://tracker.ceph.com/issues/39344>`_, `pr#27608 <https://github.com/ceph/ceph/pull/27608>`_, Sage Weil)
* mgr/prometheus: replace whitespaces in metrics' names (`pr#27886 <https://github.com/ceph/ceph/pull/27886>`_, Alfonso Mart\xc3\xadnez')
* mgr/rook: Added missing rgw daemons in service ls (`issue#39171 <http://tracker.ceph.com/issues/39171>`_, `issue#39312 <http://tracker.ceph.com/issues/39312>`_, `pr#27864 <https://github.com/ceph/ceph/pull/27864>`_, Sebastian Wagner)
* mgr/rook: Fix RGW creation (`issue#39158 <http://tracker.ceph.com/issues/39158>`_, `issue#39313 <http://tracker.ceph.com/issues/39313>`_, `pr#27863 <https://github.com/ceph/ceph/pull/27863>`_, Sebastian Wagner)
* mgr/rook: Remove support for Rook older than v0.9 (`issue#39356 <http://tracker.ceph.com/issues/39356>`_, `issue#39278 <http://tracker.ceph.com/issues/39278>`_, `pr#27862 <https://github.com/ceph/ceph/pull/27862>`_, Sebastian Wagner)
* mgr/test_orchestrator: AttributeError: 'TestWriteCompletion' object has no attribute 'id (`issue#39536 <http://tracker.ceph.com/issues/39536>`_, `pr#27920 <https://github.com/ceph/ceph/pull/27920>`_, Sebastian Wagner')
* mgr/volumes: FS subvolumes enhancements (`issue#40429 <http://tracker.ceph.com/issues/40429>`_, `pr#28767 <https://github.com/ceph/ceph/pull/28767>`_, Ramana Raja)
* mgr/volumes: add CephFS subvolumes library (`issue#39750 <http://tracker.ceph.com/issues/39750>`_, `issue#40152 <http://tracker.ceph.com/issues/40152>`_, `issue#39949 <http://tracker.ceph.com/issues/39949>`_, `issue#40014 <http://tracker.ceph.com/issues/40014>`_, `issue#39610 <http://tracker.ceph.com/issues/39610>`_, `pr#28429 <https://github.com/ceph/ceph/pull/28429>`_, Sage Weil, Venky Shankar, Ramana Raja, Rishabh Dave)
* mgr/volumes: refactor volume module (`issue#40378 <http://tracker.ceph.com/issues/40378>`_, `issue#39969 <http://tracker.ceph.com/issues/39969>`_, `pr#28595 <https://github.com/ceph/ceph/pull/28595>`_, Venky Shankar)
* mgr: Update the restful module in nautilus (`pr#28291 <https://github.com/ceph/ceph/pull/28291>`_, Kefu Chai, Boris Ranto)
* mgr: deadlock (`issue#39040 <http://tracker.ceph.com/issues/39040>`_, `issue#39425 <http://tracker.ceph.com/issues/39425>`_, `pr#28098 <https://github.com/ceph/ceph/pull/28098>`_, xie xingguo)
* mgr: fix pgp_num adjustments (`issue#38626 <http://tracker.ceph.com/issues/38626>`_, `pr#27876 <https://github.com/ceph/ceph/pull/27876>`_, Sage Weil, Marius Schiffer)
* mgr: log an error if we can't find any modules to load (`issue#40090 <http://tracker.ceph.com/issues/40090>`_, `pr#28347 <https://github.com/ceph/ceph/pull/28347>`_, Tim Serong')
* monitoring: pybind/mgr: fix format for rbd-mirror prometheus metrics (`pr#28485 <https://github.com/ceph/ceph/pull/28485>`_, Mykola Golub)
* msg/async: connection race + winner fault can leave connection stuck at replacing foreve (`issue#39241 <http://tracker.ceph.com/issues/39241>`_, `issue#37499 <http://tracker.ceph.com/issues/37499>`_, `issue#39448 <http://tracker.ceph.com/issues/39448>`_, `issue#38493 <http://tracker.ceph.com/issues/38493>`_, `pr#27915 <https://github.com/ceph/ceph/pull/27915>`_, Jason Dillaman, xie xingguo)
* msg/async/ProtocolV[12]: add ms_learn_addr_from_peer (`pr#28589 <https://github.com/ceph/ceph/pull/28589>`_, Sage Weil)
* msg: output peer address when detecting bad CRCs (`issue#39367 <http://tracker.ceph.com/issues/39367>`_, `pr#27857 <https://github.com/ceph/ceph/pull/27857>`_, Greg Farnum)
* pybind: Add 'RBD_FEATURE_MIGRATING' to rbd.pyx (`issue#39609 <http://tracker.ceph.com/issues/39609>`_, `issue#39736 <http://tracker.ceph.com/issues/39736>`_, `pr#28482 <https://github.com/ceph/ceph/pull/28482>`_, Ricardo Marques')
* pybind: Rados.get_fsid() returning bytes in python3 (`issue#40192 <http://tracker.ceph.com/issues/40192>`_, `issue#38381 <http://tracker.ceph.com/issues/38381>`_, `pr#28476 <https://github.com/ceph/ceph/pull/28476>`_, Jason Dillaman)
* rbd: krbd: fix rbd map hang due to udev return subsystem unordered (`issue#39089 <http://tracker.ceph.com/issues/39089>`_, `issue#39315 <http://tracker.ceph.com/issues/39315>`_, `pr#28019 <https://github.com/ceph/ceph/pull/28019>`_, Zhi Zhang)
* rbd: librbd: async open/close should free ImageCtx before issuing callback (`issue#39428 <http://tracker.ceph.com/issues/39428>`_, `issue#39031 <http://tracker.ceph.com/issues/39031>`_, `pr#28121 <https://github.com/ceph/ceph/pull/28121>`_, Jason Dillaman)
* rbd: librbd: avoid dereferencing an empty container during deep-copy (`issue#40368 <http://tracker.ceph.com/issues/40368>`_, `issue#40379 <http://tracker.ceph.com/issues/40379>`_, `pr#28577 <https://github.com/ceph/ceph/pull/28577>`_, Jason Dillaman)
* rbd: librbd: do not allow to deep copy migrating image (`issue#39224 <http://tracker.ceph.com/issues/39224>`_, `pr#27882 <https://github.com/ceph/ceph/pull/27882>`_, Mykola Golub)
* rbd: librbd: fix issues with object-map/fast-diff feature interlock (`issue#39946 <http://tracker.ceph.com/issues/39946>`_, `issue#39521 <http://tracker.ceph.com/issues/39521>`_, `pr#28127 <https://github.com/ceph/ceph/pull/28127>`_, Jason Dillaman)
* rbd: librbd: fixed several race conditions related to copyup (`issue#39195 <http://tracker.ceph.com/issues/39195>`_, `issue#39021 <http://tracker.ceph.com/issues/39021>`_, `pr#28132 <https://github.com/ceph/ceph/pull/28132>`_, Jason Dillaman)
* rbd: librbd: make flush be queued by QOS throttler (`issue#38869 <http://tracker.ceph.com/issues/38869>`_, `pr#28120 <https://github.com/ceph/ceph/pull/28120>`_, Mykola Golub)
* rbd: librbd: re-add support for nautilus clients talking to jewel clusters (`issue#39450 <http://tracker.ceph.com/issues/39450>`_, `pr#27936 <https://github.com/ceph/ceph/pull/27936>`_, Jason Dillaman)
* rbd: librbd: support EC data pool images sparsify (`issue#39226 <http://tracker.ceph.com/issues/39226>`_, `pr#27903 <https://github.com/ceph/ceph/pull/27903>`_, Mykola Golub)
* rbd: rbd-mirror: clear out bufferlist prior to listing mirror images (`issue#39462 <http://tracker.ceph.com/issues/39462>`_, `issue#39407 <http://tracker.ceph.com/issues/39407>`_, `pr#28122 <https://github.com/ceph/ceph/pull/28122>`_, Jason Dillaman)
* rbd: rbd-mirror: image replayer should periodically flush IO and commit positions (`issue#39257 <http://tracker.ceph.com/issues/39257>`_, `issue#39288 <http://tracker.ceph.com/issues/39288>`_, `pr#27937 <https://github.com/ceph/ceph/pull/27937>`_, Jason Dillaman)
* rgw: Evaluating bucket policies also while reading permissions for an\xe2\x80\xa6 (`issue#38638 <http://tracker.ceph.com/issues/38638>`_, `issue#39273 <http://tracker.ceph.com/issues/39273>`_, `pr#27918 <https://github.com/ceph/ceph/pull/27918>`_, Pritha Srivastava)
* rgw: admin: handle delete_at attr in object stat output (`pr#27827 <https://github.com/ceph/ceph/pull/27827>`_, Abhishek Lekshmanan)
* rgw: beast: multiple v4 and v6 endpoints with the same port will cause failure (`issue#39746 <http://tracker.ceph.com/issues/39746>`_, `issue#39038 <http://tracker.ceph.com/issues/39038>`_, `pr#28541 <https://github.com/ceph/ceph/pull/28541>`_, Abhishek Lekshmanan)
* rgw: beast: set a default port for endpoints (`issue#39048 <http://tracker.ceph.com/issues/39048>`_, `issue#39000 <http://tracker.ceph.com/issues/39000>`_, `pr#27660 <https://github.com/ceph/ceph/pull/27660>`_, Abhishek Lekshmanan)
* rgw: bucket stats report mtime in UTC (`pr#27826 <https://github.com/ceph/ceph/pull/27826>`_, Alfonso Mart\xc3\xadnez, Casey Bodley)
* rgw: clean up some logging (`issue#39503 <http://tracker.ceph.com/issues/39503>`_, `pr#27953 <https://github.com/ceph/ceph/pull/27953>`_, J. Eric Ivancich)
* rgw: cloud sync module fails to sync multipart objects (`issue#39684 <http://tracker.ceph.com/issues/39684>`_, `pr#28064 <https://github.com/ceph/ceph/pull/28064>`_, Abhishek Lekshmanan)
* rgw: cloud sync module logs attrs in the log (`issue#39574 <http://tracker.ceph.com/issues/39574>`_, `pr#27954 <https://github.com/ceph/ceph/pull/27954>`_, Nathan Cutler)
* rgw: crypto: throw DigestException from Digest and HMAC (`issue#39676 <http://tracker.ceph.com/issues/39676>`_, `issue#39456 <http://tracker.ceph.com/issues/39456>`_, `pr#28309 <https://github.com/ceph/ceph/pull/28309>`_, Matt Benjamin)
* rgw: document CreateBucketConfiguration for s3 PUT Bucket request (`issue#39597 <http://tracker.ceph.com/issues/39597>`_, `issue#39601 <http://tracker.ceph.com/issues/39601>`_, `pr#28512 <https://github.com/ceph/ceph/pull/28512>`_, Casey Bodley)
* rgw: fix Multisite sync corruption (`pr#28383 <https://github.com/ceph/ceph/pull/28383>`_, Tianshan Qu, Casey Bodley, Xiaoxi CHEN)
* rgw: fix bucket may redundantly list keys after BI_PREFIX_CHAR (`issue#39984 <http://tracker.ceph.com/issues/39984>`_, `issue#40148 <http://tracker.ceph.com/issues/40148>`_, `pr#28410 <https://github.com/ceph/ceph/pull/28410>`_, Casey Bodley, Tianshan Qu)
* rgw: fix default_placement containing "/" when storage_class is standard (`issue#39745 <http://tracker.ceph.com/issues/39745>`_, `issue#39380 <http://tracker.ceph.com/issues/39380>`_, `pr#28538 <https://github.com/ceph/ceph/pull/28538>`_, mkogan1)
* rgw: inefficient unordered bucket listing (`issue#39410 <http://tracker.ceph.com/issues/39410>`_, `issue#39393 <http://tracker.ceph.com/issues/39393>`_, `pr#27924 <https://github.com/ceph/ceph/pull/27924>`_, Casey Bodley)
* rgw: librgw: unexpected crash when creating bucket (`issue#39575 <http://tracker.ceph.com/issues/39575>`_, `pr#27955 <https://github.com/ceph/ceph/pull/27955>`_, Tao CHEN)
* rgw: limit entries in remove_olh_pending_entries() (`issue#39178 <http://tracker.ceph.com/issues/39178>`_, `issue#39118 <http://tracker.ceph.com/issues/39118>`_, `pr#27664 <https://github.com/ceph/ceph/pull/27664>`_, Casey Bodley)
* rgw: list bucket with start marker and delimiter will miss next object with char '0' (`issue#40762 <http://tracker.ceph.com/issues/40762>`_, `issue#39989 <http://tracker.ceph.com/issues/39989>`_, `pr#29022 <https://github.com/ceph/ceph/pull/29022>`_, Tianshan Qu)
* rgw: multisite log trimming only checks peers that sync from us (`issue#39283 <http://tracker.ceph.com/issues/39283>`_, `pr#27814 <https://github.com/ceph/ceph/pull/27814>`_, Casey Bodley)
* rgw: multisite: add perf counters to data sync (`issue#38549 <http://tracker.ceph.com/issues/38549>`_, `issue#38918 <http://tracker.ceph.com/issues/38918>`_, `pr#27921 <https://github.com/ceph/ceph/pull/27921>`_, Abhishek Lekshmanan, Casey Bodley)
* rgw: multisite: mismatch of bucket creation times from List Buckets (`issue#39635 <http://tracker.ceph.com/issues/39635>`_, `issue#39735 <http://tracker.ceph.com/issues/39735>`_, `pr#28444 <https://github.com/ceph/ceph/pull/28444>`_, Casey Bodley)
* rgw: multisite: period pusher gets 403 Forbidden against other zonegroups (`issue#39287 <http://tracker.ceph.com/issues/39287>`_, `issue#39414 <http://tracker.ceph.com/issues/39414>`_, `pr#27952 <https://github.com/ceph/ceph/pull/27952>`_, Casey Bodley)
* rgw: race condition between resharding and ops waiting on resharding (`issue#39202 <http://tracker.ceph.com/issues/39202>`_, `pr#27800 <https://github.com/ceph/ceph/pull/27800>`_, J. Eric Ivancich)
* rgw: radosgw-admin: add tenant argument to reshard cancel (`issue#39018 <http://tracker.ceph.com/issues/39018>`_, `pr#27630 <https://github.com/ceph/ceph/pull/27630>`_, Abhishek Lekshmanan)
* rgw: rgw_file: save etag and acl info in setattr (`issue#39228 <http://tracker.ceph.com/issues/39228>`_, `pr#27904 <https://github.com/ceph/ceph/pull/27904>`_, Tao Chen)
* rgw: swift object expiry fails when a bucket reshards (`issue#39740 <http://tracker.ceph.com/issues/39740>`_, `pr#28537 <https://github.com/ceph/ceph/pull/28537>`_, Abhishek Lekshmanan)
* rgw: unittest_rgw_dmclock_scheduler does not need Boost_LIBRARIES (`issue#39577 <http://tracker.ceph.com/issues/39577>`_, `pr#27944 <https://github.com/ceph/ceph/pull/27944>`_, Willem Jan Withagen)
* rgw: update resharding documentation (`issue#39046 <http://tracker.ceph.com/issues/39046>`_, `pr#27923 <https://github.com/ceph/ceph/pull/27923>`_, J. Eric Ivancich)
* tests: added `bluestore_warn_on_legacy_statfs: false` setting (`issue#40467 <http://tracker.ceph.com/issues/40467>`_, `pr#28723 <https://github.com/ceph/ceph/pull/28723>`_, Yuri Weinstein)
* tests: added ragweed coverage to stress-split\\* upgrade suites (`issue#40452 <http://tracker.ceph.com/issues/40452>`_, `issue#40467 <http://tracker.ceph.com/issues/40467>`_, `pr#28661 <https://github.com/ceph/ceph/pull/28661>`_, Yuri Weinstein)
* tests: added v14.2.1 (`issue#40181 <http://tracker.ceph.com/issues/40181>`_, `pr#28416 <https://github.com/ceph/ceph/pull/28416>`_, Yuri Weinstein)
* tests: cannot schedule kcephfs/multimds (`issue#40116 <http://tracker.ceph.com/issues/40116>`_, `pr#28369 <https://github.com/ceph/ceph/pull/28369>`_, Patrick Donnelly)
* tests: centos 7.6 etc (`pr#27439 <https://github.com/ceph/ceph/pull/27439>`_, Sage Weil)
* tests: ceph-ansible: ceph-ansible requires ansible 2.8 (`issue#40602 <http://tracker.ceph.com/issues/40602>`_, `issue#40669 <http://tracker.ceph.com/issues/40669>`_, `pr#28871 <https://github.com/ceph/ceph/pull/28871>`_, Brad Hubbard)
* tests: ceph-ansible: cephfs_pools variable pgs should be pg_num (`issue#40670 <http://tracker.ceph.com/issues/40670>`_, `issue#40605 <http://tracker.ceph.com/issues/40605>`_, `pr#28872 <https://github.com/ceph/ceph/pull/28872>`_, Brad Hubbard)
* tests: cephfs-shell: teuthology tests (`issue#39935 <http://tracker.ceph.com/issues/39935>`_, `issue#39526 <http://tracker.ceph.com/issues/39526>`_, `pr#28614 <https://github.com/ceph/ceph/pull/28614>`_, Milind Changire)
* tests: cephfs: TestMisc.test_evict_client fails (`issue#40220 <http://tracker.ceph.com/issues/40220>`_, `pr#28613 <https://github.com/ceph/ceph/pull/28613>`_, "Yan, Zheng")
* tests: cleaned up supported distro for nautilus (`pr#28065 <https://github.com/ceph/ceph/pull/28065>`_, Yuri Weinstein)
* tests: ignore legacy bluestore stats errors (`issue#40374 <http://tracker.ceph.com/issues/40374>`_, `pr#28563 <https://github.com/ceph/ceph/pull/28563>`_, Patrick Donnelly)
* tests: librbd: drop 'ceph_test_librbd_api' target (`issue#39423 <http://tracker.ceph.com/issues/39423>`_, `issue#39072 <http://tracker.ceph.com/issues/39072>`_, `pr#28091 <https://github.com/ceph/ceph/pull/28091>`_, Jason Dillaman')
* tests: mgr: tox failures when running make check (`issue#39323 <http://tracker.ceph.com/issues/39323>`_, `issue#39530 <http://tracker.ceph.com/issues/39530>`_, `pr#27884 <https://github.com/ceph/ceph/pull/27884>`_, Nathan Cutler)
* tests: pass --ssh-config to pytest to resolve hosts when connecting (`pr#28923 <https://github.com/ceph/ceph/pull/28923>`_, Alfredo Deza)
* tests: rbd: qemu-iotests tests fail under latest Ubuntu kernel (`issue#39541 <http://tracker.ceph.com/issues/39541>`_, `issue#24668 <http://tracker.ceph.com/issues/24668>`_, `pr#27988 <https://github.com/ceph/ceph/pull/27988>`_, Jason Dillaman)
* tests: removed `1node` and `systemd` tests as ceph-deploy is not a\xe2\x80\xa6 (`pr#28458 <https://github.com/ceph/ceph/pull/28458>`_, Yuri Weinstein)
* tests: rgw: fix race in test_rgw_reshard_wait and test_rgw_reshard_wait uses same clock for timing (`issue#39479 <http://tracker.ceph.com/issues/39479>`_, `pr#27779 <https://github.com/ceph/ceph/pull/27779>`_, Casey Bodley)
* tests: rgw: fix swift warning message (`issue#40304 <http://tracker.ceph.com/issues/40304>`_, `pr#28698 <https://github.com/ceph/ceph/pull/28698>`_, Casey Bodley)
* tests: rgw: more fixes for swift task (`issue#40304 <http://tracker.ceph.com/issues/40304>`_, `pr#28922 <https://github.com/ceph/ceph/pull/28922>`_, Casey Bodley)
* tests: rgw: skip swift tests on rhel 7.6+ (`issue#40402 <http://tracker.ceph.com/issues/40402>`_, `issue#40304 <http://tracker.ceph.com/issues/40304>`_, `pr#28604 <https://github.com/ceph/ceph/pull/28604>`_, Casey Bodley)
* tests: stop testing simple messenger in fs qa (`issue#40373 <http://tracker.ceph.com/issues/40373>`_, `pr#28562 <https://github.com/ceph/ceph/pull/28562>`_, Patrick Donnelly)
* tests: tasks/rbd_fio: fixed missing delimiter between 'cd' and 'configure (`issue#39590 <http://tracker.ceph.com/issues/39590>`_, `pr#27989 <https://github.com/ceph/ceph/pull/27989>`_, Jason Dillaman')
* tests: test_sessionmap assumes simple messenger (`issue#39430 <http://tracker.ceph.com/issues/39430>`_, `pr#27772 <https://github.com/ceph/ceph/pull/27772>`_, Patrick Donnelly)
* tests: use curl in wait_for_radosgw() in util/rgw.py (`issue#40346 <http://tracker.ceph.com/issues/40346>`_, `pr#28598 <https://github.com/ceph/ceph/pull/28598>`_, Ali Maredia)
* tests: workunits/rbd: use https protocol for devstack git operations (`issue#39656 <http://tracker.ceph.com/issues/39656>`_, `issue#39729 <http://tracker.ceph.com/issues/39729>`_, `pr#28128 <https://github.com/ceph/ceph/pull/28128>`_, Jason Dillaman)
* tests: workunits/rbd: wait for rbd-nbd unmap to complete (`issue#39675 <http://tracker.ceph.com/issues/39675>`_, `issue#39598 <http://tracker.ceph.com/issues/39598>`_, `pr#28273 <https://github.com/ceph/ceph/pull/28273>`_, Jason Dillaman)


v14.2.1 Nautilus
================

This is the first bug fix release of Ceph Nautilus release series. We recommend
all nautilus users upgrade to this release. For upgrading from older releases of
ceph, general guidelines for upgrade to nautilus must be followed
:ref:`nautilus-old-upgrade`.

Notable Changes
---------------

* Ceph now packages python bindings for python3.6 instead of
  python3.4, because EPEL7 recently switched from python3.4 to
  python3.6 as the native python3. see the `announcement <https://lists.fedoraproject.org/archives/list/epel-announce@lists.fedoraproject.org/message/EGUMKAIMPK2UD5VSHXM53BH2MBDGDWMO/>`_
  for more details on the background of this change.

Known Issues
------------

* Nautilus-based librbd clients cannot open images stored on pre-Luminous
  clusters

Changelog
---------
* bluestore: ceph-bluestore-tool: bluefs-bdev-expand cmd might assert if no WAL is configured (`issue#39253 <http://tracker.ceph.com/issues/39253>`_, `pr#27523 <https://github.com/ceph/ceph/pull/27523>`_, Igor Fedotov)
* bluestore: os/bluestore: fix bitmap allocator issues (`pr#27139 <https://github.com/ceph/ceph/pull/27139>`_, Igor Fedotov)
* build/ops,rgw: rgw: build async scheduler only when beast is built (`pr#27191 <https://github.com/ceph/ceph/pull/27191>`_, Abhishek Lekshmanan)
* build/ops: build/ops: Running ceph under Pacemaker control not supported by SUSE Linux Enterprise (`issue#38862 <http://tracker.ceph.com/issues/38862>`_, `pr#27127 <https://github.com/ceph/ceph/pull/27127>`_, Nathan Cutler)
* build/ops: build/ops: ceph-mgr-diskprediction-local requires numpy and scipy on SUSE, but these packages do not exist on SUSE (`issue#38863 <http://tracker.ceph.com/issues/38863>`_, `pr#27125 <https://github.com/ceph/ceph/pull/27125>`_, Nathan Cutler)
* build/ops: cmake/FindRocksDB: fix IMPORTED_LOCATION for ROCKSDB_LIBRARIES (`issue#38993 <http://tracker.ceph.com/issues/38993>`_, `pr#27601 <https://github.com/ceph/ceph/pull/27601>`_, dudengke)
* build/ops: cmake: revert librados_tp.so version from 3 to 2 (`issue#39291 <http://tracker.ceph.com/issues/39291>`_, `issue#39293 <http://tracker.ceph.com/issues/39293>`_, `pr#27597 <https://github.com/ceph/ceph/pull/27597>`_, Nathan Cutler)
* build/ops: qa,rpm,cmake: switch over to python3.6 (`issue#39236 <http://tracker.ceph.com/issues/39236>`_, `issue#39164 <http://tracker.ceph.com/issues/39164>`_, `pr#27505 <https://github.com/ceph/ceph/pull/27505>`_, Boris Ranto, Kefu Chai)
* cephfs: fs: we lack a feature bit for nautilus (`issue#39078 <http://tracker.ceph.com/issues/39078>`_, `issue#39187 <http://tracker.ceph.com/issues/39187>`_, `pr#27497 <https://github.com/ceph/ceph/pull/27497>`_, Patrick Donnelly)
* cephfs: ls -S command produces AttributeError: 'str' object has no attribute 'decode' (`pr#27531 <https://github.com/ceph/ceph/pull/27531>`_, Varsha Rao)
* cephfs: mds|kclient: MDS_CLIENT_LATE_RELEASE warning caused by inline bug on RHEL 7.5 (`issue#39225 <http://tracker.ceph.com/issues/39225>`_, `pr#27500 <https://github.com/ceph/ceph/pull/27500>`_, "Yan, Zheng")
* common,core: crush: various fixes for weight-sets, the osd_crush_update_weight_set option, and tests (`pr#27119 <https://github.com/ceph/ceph/pull/27119>`_, Sage Weil)
* common/blkdev: get_device_id: behave if model is lvm and id_model_enc isn't there (`pr#27158 <https://github.com/ceph/ceph/pull/27158>`_, Sage Weil)
* common/config: parse --default-$option as a default value (`pr#27217 <https://github.com/ceph/ceph/pull/27217>`_, Sage Weil)
* core,mgr: mgr: autoscale down can lead to max_pg_per_osd limit (`issue#39271 <http://tracker.ceph.com/issues/39271>`_, `issue#38786 <http://tracker.ceph.com/issues/38786>`_, `pr#27547 <https://github.com/ceph/ceph/pull/27547>`_, Sage Weil)
* core,mon: mon/Monitor.cc: print min_mon_release correctly (`pr#27168 <https://github.com/ceph/ceph/pull/27168>`_, Neha Ojha)
* core,tests: tests: osd-markdown.sh can fail with CLI_DUP_COMMAND=1 (`issue#38359 <http://tracker.ceph.com/issues/38359>`_, `issue#39275 <http://tracker.ceph.com/issues/39275>`_, `pr#27550 <https://github.com/ceph/ceph/pull/27550>`_, Sage Weil)
* core: Improvements to auto repair (`issue#38616 <http://tracker.ceph.com/issues/38616>`_, `pr#27220 <https://github.com/ceph/ceph/pull/27220>`_, xie xingguo, David Zafman)
* core: Rook: Fix creation of Bluestore OSDs (`issue#39167 <http://tracker.ceph.com/issues/39167>`_, `issue#39062 <http://tracker.ceph.com/issues/39062>`_, `pr#27486 <https://github.com/ceph/ceph/pull/27486>`_, Sebastian Wagner)
* core: ceph-objectstore-tool: rename dump-import to dump-export (`issue#39325 <http://tracker.ceph.com/issues/39325>`_, `issue#39284 <http://tracker.ceph.com/issues/39284>`_, `pr#27610 <https://github.com/ceph/ceph/pull/27610>`_, David Zafman)
* core: common/blkdev: handle devices with ID_MODEL as "LVM PV ..." but valid ID_MODEL_ENC (`pr#27096 <https://github.com/ceph/ceph/pull/27096>`_, Sage Weil)
* core: common: fix deferred log starting (`pr#27388 <https://github.com/ceph/ceph/pull/27388>`_, Sage Weil, Jason Dillaman)
* core: crush/CrushCompiler: Fix __replacement_assert (`issue#39174 <http://tracker.ceph.com/issues/39174>`_, `pr#27620 <https://github.com/ceph/ceph/pull/27620>`_, Brad Hubbard)
* core: global: explicitly call out EIO events in crash dumps (`pr#27440 <https://github.com/ceph/ceph/pull/27440>`_, Sage Weil)
* core: log: log_to_file + --default-\* + fixes and improvements (`pr#27278 <https://github.com/ceph/ceph/pull/27278>`_, Sage Weil)
* core: mon/MgrStatMonitor: ensure only one copy of initial service map (`issue#38839 <http://tracker.ceph.com/issues/38839>`_, `pr#27116 <https://github.com/ceph/ceph/pull/27116>`_, Sage Weil)
* core: mon/OSDMonitor: allow 'osd pool set pgp_num_actual' (`pr#27060 <https://github.com/ceph/ceph/pull/27060>`_, Sage Weil)
* core: mon: make mon_osd_down_out_subtree_limit update at runtime (`pr#27582 <https://github.com/ceph/ceph/pull/27582>`_, Sage Weil)
* core: mon: ok-to-stop commands for mon and mds (`pr#27347 <https://github.com/ceph/ceph/pull/27347>`_, Sage Weil)
* core: mon: quiet devname log noise (`pr#27314 <https://github.com/ceph/ceph/pull/27314>`_, Sage Weil)
* core: osd/OSDMap: add 'zone' to default crush map (`pr#27117 <https://github.com/ceph/ceph/pull/27117>`_, Sage Weil)
* core: osd/PGLog.h: print olog_can_rollback_to before deciding to rollback (`issue#38906 <http://tracker.ceph.com/issues/38906>`_, `issue#38894 <http://tracker.ceph.com/issues/38894>`_, `pr#27302 <https://github.com/ceph/ceph/pull/27302>`_, Neha Ojha)
* core: osd/osd_types: fix object_stat_sum_t fast-path decode (`issue#39320 <http://tracker.ceph.com/issues/39320>`_, `issue#39281 <http://tracker.ceph.com/issues/39281>`_, `pr#27555 <https://github.com/ceph/ceph/pull/27555>`_, David Zafman)
* core: osd: backport recent upmap fixes (`issue#38860 <http://tracker.ceph.com/issues/38860>`_, `issue#38967 <http://tracker.ceph.com/issues/38967>`_, `issue#38897 <http://tracker.ceph.com/issues/38897>`_, `issue#38826 <http://tracker.ceph.com/issues/38826>`_, `pr#27225 <https://github.com/ceph/ceph/pull/27225>`_, huangjun, xie xingguo)
* core: osd: process_copy_chunk remove obc ref before pg unlock (`issue#38842 <http://tracker.ceph.com/issues/38842>`_, `issue#38973 <http://tracker.ceph.com/issues/38973>`_, `pr#27478 <https://github.com/ceph/ceph/pull/27478>`_, Zengran Zhang)
* dashboard: NFS: failed to disable NFSv3 in export create (`issue#39104 <http://tracker.ceph.com/issues/39104>`_, `issue#38997 <http://tracker.ceph.com/issues/38997>`_, `pr#27368 <https://github.com/ceph/ceph/pull/27368>`_, Tiago Melo)
* doc/releases/nautilus: fix config update step (`pr#27502 <https://github.com/ceph/ceph/pull/27502>`_, Sage Weil)
* doc: doc/orchestrator: Fix broken bullet points (`issue#39168 <http://tracker.ceph.com/issues/39168>`_, `pr#27487 <https://github.com/ceph/ceph/pull/27487>`_, Sebastian Wagner)
* doc: doc: Minor rados related documentation fixes (`issue#38896 <http://tracker.ceph.com/issues/38896>`_, `issue#38903 <http://tracker.ceph.com/issues/38903>`_, `pr#27189 <https://github.com/ceph/ceph/pull/27189>`_, David Zafman)
* doc: doc: rgw: Added library/package for Golang (`issue#38730 <http://tracker.ceph.com/issues/38730>`_, `issue#38867 <http://tracker.ceph.com/issues/38867>`_, `pr#27549 <https://github.com/ceph/ceph/pull/27549>`_, Irek Fasikhov)
* install-deps.sh: install '\*rpm-macros' (`issue#39164 <http://tracker.ceph.com/issues/39164>`_, `pr#27544 <https://github.com/ceph/ceph/pull/27544>`_, Kefu Chai)
* mgr/dashboard add polish language (`issue#39052 <http://tracker.ceph.com/issues/39052>`_, `pr#27287 <https://github.com/ceph/ceph/pull/27287>`_, Sebastian Krah)
* mgr/dashboard/qa: Improve tasks.mgr.test_dashboard.TestDashboard.test_standby (`pr#27237 <https://github.com/ceph/ceph/pull/27237>`_, Volker Theile)
* mgr/dashboard: 1 osds exist in the crush map but not in the osdmap breaks OSD page (`issue#38885 <http://tracker.ceph.com/issues/38885>`_, `issue#36086 <http://tracker.ceph.com/issues/36086>`_, `pr#27543 <https://github.com/ceph/ceph/pull/27543>`_, Patrick Nawracay)
* mgr/dashboard: Adapt iSCSI overview page to make use of ceph-iscsi (`pr#27541 <https://github.com/ceph/ceph/pull/27541>`_, Ricardo Marques)
* mgr/dashboard: Add date range and log search functionality (`issue#37387 <http://tracker.ceph.com/issues/37387>`_, `issue#38878 <http://tracker.ceph.com/issues/38878>`_, `pr#27283 <https://github.com/ceph/ceph/pull/27283>`_, guodan1)
* mgr/dashboard: Add refresh interval to the dashboard landing page (`issue#26872 <http://tracker.ceph.com/issues/26872>`_, `issue#38988 <http://tracker.ceph.com/issues/38988>`_, `pr#27267 <https://github.com/ceph/ceph/pull/27267>`_, guodan1)
* mgr/dashboard: Add separate option to config SSL port (`issue#39001 <http://tracker.ceph.com/issues/39001>`_, `pr#27393 <https://github.com/ceph/ceph/pull/27393>`_, Volker Theile)
* mgr/dashboard: Added breadcrumb tests to NFS menu (`issue#38981 <http://tracker.ceph.com/issues/38981>`_, `pr#27589 <https://github.com/ceph/ceph/pull/27589>`_, Nathan Weinberg)
* mgr/dashboard: Back button component (`issue#39058 <http://tracker.ceph.com/issues/39058>`_, `pr#27405 <https://github.com/ceph/ceph/pull/27405>`_, Stephan Müller)
* mgr/dashboard: Cannot submit NFS export form when NFSv4 is not selected (`issue#39105 <http://tracker.ceph.com/issues/39105>`_, `issue#39063 <http://tracker.ceph.com/issues/39063>`_, `pr#27370 <https://github.com/ceph/ceph/pull/27370>`_, Tiago Melo)
* mgr/dashboard: Error creating NFS export without UDP (`issue#39107 <http://tracker.ceph.com/issues/39107>`_, `issue#39090 <http://tracker.ceph.com/issues/39090>`_, `pr#27372 <https://github.com/ceph/ceph/pull/27372>`_, Tiago Melo)
* mgr/dashboard: Error on iSCSI disk diff (`pr#27460 <https://github.com/ceph/ceph/pull/27460>`_, Ricardo Marques)
* mgr/dashboard: Filter iSCSI target images based on required features (`issue#39002 <http://tracker.ceph.com/issues/39002>`_, `pr#27363 <https://github.com/ceph/ceph/pull/27363>`_, Ricardo Marques)
* mgr/dashboard: Fix env vars of `run-tox.sh` (`issue#38798 <http://tracker.ceph.com/issues/38798>`_, `issue#38864 <http://tracker.ceph.com/issues/38864>`_, `pr#27361 <https://github.com/ceph/ceph/pull/27361>`_, Patrick Nawracay)
* mgr/dashboard: Fixes tooltip behavior (`pr#27395 <https://github.com/ceph/ceph/pull/27395>`_, Stephan Müller)
* mgr/dashboard: FixtureHelper (`issue#39041 <http://tracker.ceph.com/issues/39041>`_, `pr#27398 <https://github.com/ceph/ceph/pull/27398>`_, Stephan Müller)
* mgr/dashboard: NFS Squash field should be required (`issue#39106 <http://tracker.ceph.com/issues/39106>`_, `issue#39064 <http://tracker.ceph.com/issues/39064>`_, `pr#27371 <https://github.com/ceph/ceph/pull/27371>`_, Tiago Melo)
* mgr/dashboard: PreventDefault isn't working on 400 errors (`pr#27389 <https://github.com/ceph/ceph/pull/27389>`_, Stephan Müller)
* mgr/dashboard: Typo in "CephFS Name" field on NFS form (`issue#39067 <http://tracker.ceph.com/issues/39067>`_, `pr#27449 <https://github.com/ceph/ceph/pull/27449>`_, Tiago Melo)
* mgr/dashboard: dashboard giving 401 unauthorized (`issue#38871 <http://tracker.ceph.com/issues/38871>`_, `pr#27219 <https://github.com/ceph/ceph/pull/27219>`_, ming416)
* mgr/dashboard: fix sparkline component (`issue#38866 <http://tracker.ceph.com/issues/38866>`_, `pr#27260 <https://github.com/ceph/ceph/pull/27260>`_, Alfonso Martínez)
* mgr/dashboard: readonly user can't see any pages (`issue#39240 <http://tracker.ceph.com/issues/39240>`_, `pr#27611 <https://github.com/ceph/ceph/pull/27611>`_, Stephan Müller)
* mgr/dashboard: unify button/URL actions naming + bugfix (add whitelist to guard) (`issue#37337 <http://tracker.ceph.com/issues/37337>`_, `issue#39003 <http://tracker.ceph.com/issues/39003>`_, `pr#27492 <https://github.com/ceph/ceph/pull/27492>`_, Ernesto Puerta)
* mgr/dashboard: update vstart to use new ssl_server_port (`issue#39124 <http://tracker.ceph.com/issues/39124>`_, `pr#27394 <https://github.com/ceph/ceph/pull/27394>`_, Ernesto Puerta)
* mgr/deepsea: use ceph_volume output in get_inventory() (`issue#39083 <http://tracker.ceph.com/issues/39083>`_, `pr#27319 <https://github.com/ceph/ceph/pull/27319>`_, Tim Serong)
* mgr/diskprediction_cloud: Correct base64 encode translate table (`pr#27167 <https://github.com/ceph/ceph/pull/27167>`_, Rick Chen)
* mgr/orchestrator: Add error handling to interface (`issue#38837 <http://tracker.ceph.com/issues/38837>`_, `pr#27095 <https://github.com/ceph/ceph/pull/27095>`_, Sebastian Wagner)
* mgr/pg_autoscaler: add pg_autoscale_bias (`pr#27387 <https://github.com/ceph/ceph/pull/27387>`_, Sage Weil)
* mgr:  mgr/dashboard: Error on iSCSI target submission (`pr#27461 <https://github.com/ceph/ceph/pull/27461>`_, Ricardo Marques)
* mgr: ceph-mgr:  ImportError: Interpreter change detected - this module can only be loaded into one interprer per process (`issue#38865 <http://tracker.ceph.com/issues/38865>`_, `pr#27128 <https://github.com/ceph/ceph/pull/27128>`_, Tim Serong)
* mgr: mgr/DaemonServer: handle_conf_change - fix broken locking (`issue#38964 <http://tracker.ceph.com/issues/38964>`_, `issue#38899 <http://tracker.ceph.com/issues/38899>`_, `pr#27454 <https://github.com/ceph/ceph/pull/27454>`_, xie xingguo)
* mgr: mgr/balancer: Python 3 compatibility fix (`issue#38831 <http://tracker.ceph.com/issues/38831>`_, `issue#38855 <http://tracker.ceph.com/issues/38855>`_, `pr#27227 <https://github.com/ceph/ceph/pull/27227>`_, Marius Schiffer)
* mgr: mgr/dashboard: Check if gateway is in use before allowing the deletion via `iscsi-gateway-rm` command (`pr#27457 <https://github.com/ceph/ceph/pull/27457>`_, Ricardo Marques)
* mgr: mgr/dashboard: Display the number of active sessions for each iSCSI target (`pr#27450 <https://github.com/ceph/ceph/pull/27450>`_, Ricardo Marques)
* mgr: mgr/devicehealth: Fix python 3 incompatiblity (`issue#38957 <http://tracker.ceph.com/issues/38957>`_, `issue#38939 <http://tracker.ceph.com/issues/38939>`_, `pr#27390 <https://github.com/ceph/ceph/pull/27390>`_, Marius Schiffer)
* mgr: mgr/telemetry: add report_timestamp to sent reports (`pr#27701 <https://github.com/ceph/ceph/pull/27701>`_, Dan Mick)
* mgr: mgr/telemetry: use list; redact host; 24h default interval (`pr#27709 <https://github.com/ceph/ceph/pull/27709>`_, Sage Weil, Dan Mick)
* mgr: mgr: Configure Py root logger for Mgr modules (`issue#38969 <http://tracker.ceph.com/issues/38969>`_, `pr#27261 <https://github.com/ceph/ceph/pull/27261>`_, Volker Theile)
* mgr: mgr: Diskprediction unable to transfer data into the cloud server (`issue#38970 <http://tracker.ceph.com/issues/38970>`_, `pr#27240 <https://github.com/ceph/ceph/pull/27240>`_, Rick Chen)
* mon/MonClient: do not dereference auth_supported.end() (`pr#27215 <https://github.com/ceph/ceph/pull/27215>`_, Kefu Chai)
* mon/MonmapMonitor: clean up empty created stamp in monmap (`issue#39085 <http://tracker.ceph.com/issues/39085>`_, `pr#27399 <https://github.com/ceph/ceph/pull/27399>`_, Sage Weil)
* mon: mon: add cluster log to file option (`pr#27346 <https://github.com/ceph/ceph/pull/27346>`_, Sage Weil)
* msg/async v2: make v2 work on rdma (`pr#27216 <https://github.com/ceph/ceph/pull/27216>`_, Jianpeng Ma)
* msg: default to debug_ms=0 (`pr#27197 <https://github.com/ceph/ceph/pull/27197>`_, Sage Weil)
* osd: OSDMapRef access by multiple threads is unsafe (`pr#27402 <https://github.com/ceph/ceph/pull/27402>`_, Zengran Zhang, Kefu Chai)
* qa/valgrind (`pr#27320 <https://github.com/ceph/ceph/pull/27320>`_, Radoslaw Zarzynski)
* rbd,tests: backport krbd discard qa fixes to nautilus (`issue#38861 <http://tracker.ceph.com/issues/38861>`_, `pr#27258 <https://github.com/ceph/ceph/pull/27258>`_, Ilya Dryomov)
* rbd,tests: backport krbd discard qa fixes to stable branches (`issue#38956 <http://tracker.ceph.com/issues/38956>`_, `pr#27239 <https://github.com/ceph/ceph/pull/27239>`_, Ilya Dryomov)
* rbd: librbd: ignore -EOPNOTSUPP errors when retrieving image group membership (`issue#38834 <http://tracker.ceph.com/issues/38834>`_, `pr#27080 <https://github.com/ceph/ceph/pull/27080>`_, Jason Dillaman)
* rbd: librbd: look for pool metadata in default namespace (`issue#38961 <http://tracker.ceph.com/issues/38961>`_, `pr#27423 <https://github.com/ceph/ceph/pull/27423>`_, Mykola Golub)
* rbd: librbd: trash move return EBUSY instead of EINVAL for migrating image (`issue#38968 <http://tracker.ceph.com/issues/38968>`_, `pr#27475 <https://github.com/ceph/ceph/pull/27475>`_, Mykola Golub)
* rbd: rbd: krbd: return -ETIMEDOUT in polling (`issue#38792 <http://tracker.ceph.com/issues/38792>`_, `issue#38977 <http://tracker.ceph.com/issues/38977>`_, `pr#27539 <https://github.com/ceph/ceph/pull/27539>`_, Dongsheng Yang)
* rgw: Adding tcp_nodelay option to Beast (`issue#38926 <http://tracker.ceph.com/issues/38926>`_, `pr#27355 <https://github.com/ceph/ceph/pull/27355>`_, Or Friedmann)
* rgw: Fix S3 compatibility bug when CORS is not found (`issue#38923 <http://tracker.ceph.com/issues/38923>`_, `issue#37945 <http://tracker.ceph.com/issues/37945>`_, `pr#27331 <https://github.com/ceph/ceph/pull/27331>`_, Nick Janus)
* rgw: LC: handle resharded buckets (`pr#27559 <https://github.com/ceph/ceph/pull/27559>`_, Abhishek Lekshmanan)
* rgw: Make rgw admin ops api get user info consistent with the command line (`issue#39135 <http://tracker.ceph.com/issues/39135>`_, `pr#27501 <https://github.com/ceph/ceph/pull/27501>`_, Li Shuhao)
* rgw: don't crash on missing /etc/mime.types (`issue#38921 <http://tracker.ceph.com/issues/38921>`_, `issue#38328 <http://tracker.ceph.com/issues/38328>`_, `pr#27329 <https://github.com/ceph/ceph/pull/27329>`_, Casey Bodley)
* rgw: don't recalculate etags for slo/dlo (`pr#27561 <https://github.com/ceph/ceph/pull/27561>`_, Casey Bodley)
* rgw: fix RGWDeleteMultiObj::verify_permission() (`issue#38980 <http://tracker.ceph.com/issues/38980>`_, `pr#27586 <https://github.com/ceph/ceph/pull/27586>`_, Irek Fasikhov)
* rgw: fix read not exists null version return wrong (`issue#38811 <http://tracker.ceph.com/issues/38811>`_, `issue#38909 <http://tracker.ceph.com/issues/38909>`_, `pr#27306 <https://github.com/ceph/ceph/pull/27306>`_, Tianshan Qu)
* rgw: ldap: fix early return in LDAPAuthEngine::init w/uri not empty() (`issue#38754 <http://tracker.ceph.com/issues/38754>`_, `pr#26972 <https://github.com/ceph/ceph/pull/26972>`_, Matt Benjamin)
* rgw: multisite: data sync loops back to the start of the datalog after reaching the end (`issue#39075 <http://tracker.ceph.com/issues/39075>`_, `issue#39033 <http://tracker.ceph.com/issues/39033>`_, `pr#27498 <https://github.com/ceph/ceph/pull/27498>`_, Casey Bodley)
* rgw: nfs: skip empty (non-POSIX) path segments (`issue#38744 <http://tracker.ceph.com/issues/38744>`_, `issue#38773 <http://tracker.ceph.com/issues/38773>`_, `pr#27208 <https://github.com/ceph/ceph/pull/27208>`_, Matt Benjamin)
* rgw: nfs: svc-enable RGWLib (`issue#38774 <http://tracker.ceph.com/issues/38774>`_, `pr#27232 <https://github.com/ceph/ceph/pull/27232>`_, Matt Benjamin)
* rgw: orphans find perf improvements (`issue#39181 <http://tracker.ceph.com/issues/39181>`_, `pr#27560 <https://github.com/ceph/ceph/pull/27560>`_, Abhishek Lekshmanan)
* rgw: rgw admin: disable stale instance deletion in multisite (`issue#39015 <http://tracker.ceph.com/issues/39015>`_, `pr#27602 <https://github.com/ceph/ceph/pull/27602>`_, Abhishek Lekshmanan)
* rgw: sse c fixes (`issue#38700 <http://tracker.ceph.com/issues/38700>`_, `pr#27296 <https://github.com/ceph/ceph/pull/27296>`_, Adam Kupczyk, Casey Bodley, Abhishek Lekshmanan)
* rgw: support delimiter longer then one symbol (`issue#38777 <http://tracker.ceph.com/issues/38777>`_, `pr#27548 <https://github.com/ceph/ceph/pull/27548>`_, Matt Benjamin)
* rook-ceph-system namespace hardcoded in the rook orchestrator (`issue#38799 <http://tracker.ceph.com/issues/38799>`_, `issue#39250 <http://tracker.ceph.com/issues/39250>`_, `pr#27496 <https://github.com/ceph/ceph/pull/27496>`_, Sebastian Wagner)
* rpm,cmake: use specified python3 version if any (`pr#27382 <https://github.com/ceph/ceph/pull/27382>`_, Kefu Chai)


v14.2.0 Nautilus
================

This is the first stable release of Ceph Nautilus.

Major Changes from Mimic
------------------------

- *Dashboard*:

  The :ref:`mgr-dashboard` has gained a lot of new functionality:

  * Support for multiple users / roles
  * SSO (SAMLv2) for user authentication
  * Auditing support
  * New landing page, showing more metrics and health info
  * I18N support
  * REST API documentation with Swagger API

  New Ceph management features include:

  * OSD management (mark as down/out, change OSD settings, recovery profiles)
  * Cluster config settings editor
  * Ceph Pool management (create/modify/delete)
  * ECP management
  * RBD mirroring configuration
  * Embedded Grafana Dashboards (derived from Ceph Metrics)
  * CRUSH map viewer
  * NFS Ganesha management
  * iSCSI target management (via :ref:`ceph-iscsi`)
  * RBD QoS configuration
  * Ceph Manager (ceph-mgr) module management
  * Prometheus alert Management

  Also, the Ceph Dashboard is now split into its own package named
  ``ceph-mgr-dashboard``. You might want to install it separately,
  if your package management software fails to do so when it installs
  ``ceph-mgr``.

- *RADOS*:

  * The number of placement groups (PGs) per pool can now be decreased
    at any time, and the cluster can :ref:`automatically tune the PG count <pg-autoscaler>`
    based on cluster utilization or administrator hints.
  * The new :ref:`v2 wire protocol <msgr2>` brings support for encryption on the wire.
  * Physical :ref:`storage devices <devices>` consumed by OSD and Monitor daemons are
    now tracked by the cluster along with health metrics (i.e.,
    SMART), and the cluster can apply a pre-trained prediction model
    or a cloud-based prediction service to :ref:`warn about expected
    HDD or SSD failures <diskprediction>`.
  * The NUMA node for OSD daemons can easily be monitored via the
    ``ceph osd numa-status`` command, and configured via the
    ``osd_numa_node`` config option.
  * When BlueStore OSDs are used, space utilization is now broken down
    by object data, omap data, and internal metadata, by pool, and by
    pre- and post- compression sizes.
  * OSDs more effectively prioritize the most important PGs and
    objects when performing recovery and backfill.
  * Progress for long-running background processes--like recovery
    after a device failure--is now reported as part of ``ceph
    status``.
  * An experimental `Coupled-Layer "Clay" erasure code
    <https://www.usenix.org/conference/fast18/presentation/vajha>`_
    plugin has been added that reduces network bandwidth and IO needed
    for most recovery operations.

- *RGW*:

  * S3 lifecycle transition for tiering between storage classes.
  * A new web frontend (Beast) has replaced civetweb as the default,
    improving overall performance.
  * A new publish/subscribe infrastructure allows RGW to feed events
    to serverless frameworks like knative or data pipelies like Kafka.
  * A range of authentication features, including STS federation using
    OAuth2 and OpenID::connect and an OPA (Open Policy Agent)
    authentication delegation prototype.
  * The new archive zone federation feature enables full preservation
    of all objects (including history) in a separate zone.

- *CephFS*:

  * MDS stability has been greatly improved for large caches and
    long-running clients with a lot of RAM. Cache trimming and client
    capability recall is now throttled to prevent overloading the MDS.
  * CephFS may now be exported via NFS-Ganesha clusters in environments managed
    by Rook. Ceph manages the clusters and ensures high-availability and
    scalability. An `introductory demo
    <https://ceph.com/community/deploying-a-cephnfs-server-cluster-with-rook/>`_
    is available. More automation of this feature is expected to be forthcoming
    in future minor releases of Nautilus.
  * The MDS ``mds_standby_for_*``, ``mon_force_standby_active``, and
    ``mds_standby_replay`` configuration options have been obsoleted. Instead,
    the operator :ref:`may now set <mds-standby-replay>` the new
    ``allow_standby_replay`` flag on the CephFS file system. This setting
    causes standbys to become standby-replay for any available rank in the file
    system.
  * MDS now supports dropping its cache which concurrently asks clients
    to trim their caches. This is done using MDS admin socket ``cache drop``
    command.
  * It is now possible to check the progress of an on-going scrub in the MDS.
    Additionally, a scrub may be paused or aborted. See :ref:`the scrub
    documentation <mds-scrub>` for more information.
  * A new interface for creating volumes is provided via the ``ceph volume``
    command-line-interface.
  * A new cephfs-shell tool is available for manipulating a CephFS file
    system without mounting.
  * CephFS-related output from ``ceph status`` has been reformatted for brevity,
    clarity, and usefulness.
  * Lazy IO has been revamped. It can be turned on by the client using the new
    CEPH_O_LAZY flag to the ``ceph_open`` C/C++ API or via the config option
    ``client_force_lazyio``.
  * CephFS file system can now be brought down rapidly via the ``ceph fs fail``
    command. See :ref:`the administration page <cephfs-administration>` for
    more information.

- *RBD*:

  * Images can be live-migrated with minimal downtime to assist with moving
    images between pools or to new layouts.
  * New ``rbd perf image iotop`` and ``rbd perf image iostat`` commands provide
    an iotop- and iostat-like IO monitor for all RBD images.
  * The *ceph-mgr* Prometheus exporter now optionally includes an IO monitor
    for all RBD images.
  * Support for separate image namespaces within a pool for tenant isolation.

- *Misc*:

  * Ceph has a new set of :ref:`orchestrator modules
    <orchestrator-cli-module>` to directly interact with external
    orchestrators like ceph-ansible, DeepSea, Rook, or simply ssh via
    a consistent CLI (and, eventually, Dashboard) interface.

.. _nautilus-old-upgrade:

Upgrading from Mimic or Luminous
--------------------------------

Notes
~~~~~

* During the upgrade from Luminous to Nautilus, it will not be
  possible to create a new OSD using a Luminous ceph-osd daemon after
  the monitors have been upgraded to Nautilus.  We recommend you avoid adding
  or replacing any OSDs while the upgrade is in progress.

* We recommend you avoid creating any RADOS pools while the upgrade is
  in progress.

* You can monitor the progress of your upgrade at each stage with the
  ``ceph versions`` command, which will tell you what ceph version(s) are
  running for each type of daemon.

Instructions
~~~~~~~~~~~~

#. If your cluster was originally installed with a version prior to
   Luminous, ensure that it has completed at least one full scrub of
   all PGs while running Luminous.  Failure to do so will cause your
   monitor daemons to refuse to join the quorum on start, leaving them
   non-functional.

   If you are unsure whether or not your Luminous cluster has
   completed a full scrub of all PGs, you can check your cluster's
   state by running::

     # ceph osd dump | grep ^flags

   In order to be able to proceed to Nautilus, your OSD map must include
   the ``recovery_deletes`` and ``purged_snapdirs`` flags.

   If your OSD map does not contain both these flags, you can simply
   wait for approximately 24-48 hours, which in a standard cluster
   configuration should be ample time for all your placement groups to
   be scrubbed at least once, and then repeat the above process to
   recheck.

   However, if you have just completed an upgrade to Luminous and want
   to proceed to Mimic in short order, you can force a scrub on all
   placement groups with a one-line shell command, like::

     # ceph pg dump pgs_brief | cut -d " " -f 1 | xargs -n1 ceph pg scrub

   You should take into consideration that this forced scrub may
   possibly have a negative impact on your Ceph clients' performance.

#. Make sure your cluster is stable and healthy (no down or
   recovering OSDs).  (Optional, but recommended.)

#. Set the ``noout`` flag for the duration of the upgrade. (Optional,
   but recommended.)::

     # ceph osd set noout

#. Upgrade monitors by installing the new packages and restarting the
   monitor daemons.  For example, on each monitor host,::

     # systemctl restart ceph-mon.target

   Once all monitors are up, verify that the monitor upgrade is
   complete by looking for the ``nautilus`` string in the mon
   map.  The command::

     # ceph mon dump | grep min_mon_release

   should report::

     min_mon_release 14 (nautilus)

   If it doesn't, that implies that one or more monitors hasn't been
   upgraded and restarted and/or the quorum does not include all monitors.

#. Upgrade ``ceph-mgr`` daemons by installing the new packages and
   restarting all manager daemons.  For example, on each manager host,::

     # systemctl restart ceph-mgr.target

   Please note, if you are using Ceph Dashboard, you will probably need to
   install ``ceph-mgr-dashboard`` separately after upgrading ``ceph-mgr``
   package. The install script of ``ceph-mgr-dashboard`` will restart the
   manager daemons automatically for you. So in this case, you can just skip
   the step to restart the daemons.

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

   You can monitor the progress of the OSD upgrades with the
   ``ceph versions`` or ``ceph osd versions`` commands::

     # ceph osd versions
     {
        "ceph version 13.2.5 (...) mimic (stable)": 12,
        "ceph version 14.2.0 (...) nautilus (stable)": 22,
     }

#. If there are any OSDs in the cluster deployed with ceph-disk (e.g.,
   almost any OSDs that were created before the Mimic release), you
   need to tell ceph-volume to adopt responsibility for starting the
   daemons.  On each host containing OSDs, ensure the OSDs are
   currently running, and then::

     # ceph-volume simple scan
     # ceph-volume simple activate --all

   We recommend that each OSD host be rebooted following this step to
   verify that the OSDs start up automatically.

   Note that ceph-volume doesn't have the same hot-plug capability
   that ceph-disk did, where a newly attached disk is automatically
   detected via udev events.  If the OSD isn't currently running when the
   above ``scan`` command is run, or a ceph-disk-based OSD is moved to
   a new host, or the host OSD is reinstalled, or the
   ``/etc/ceph/osd`` directory is lost, you will need to scan the main
   data partition for each ceph-disk OSD explicitly.  For example,::

     # ceph-volume simple scan /dev/sdb1

   The output will include the appopriate ``ceph-volume simple
   activate`` command to enable the OSD.

#. Upgrade all CephFS MDS daemons.  For each CephFS file system,

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

#. Complete the upgrade by disallowing pre-Nautilus OSDs and enabling
   all new Nautilus-only functionality::

     # ceph osd require-osd-release nautilus

   .. important:: This step is mandatory. Failure to execute this step will make it impossible for OSDs to communicate after msgrv2 is enabled.

#. If you set ``noout`` at the beginning, be sure to clear it with::

     # ceph osd unset noout

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

#. To enable the new :ref:`v2 network protocol <msgr2>`, issue the
   following command::

     ceph mon enable-msgr2

   This will instruct all monitors that bind to the old default port
   6789 for the legacy v1 protocol to also bind to the new 3300 v2
   protocol port.  To see if all monitors have been updated,::

     ceph mon dump

   and verify that each monitor has both a ``v2:`` and ``v1:`` address
   listed.

   Running nautilus OSDs will not bind to their v2 address automatically.
   They must be restarted for that to happen.

   .. important:: 
      Before this step is run, the following command must already have been run:

      # ceph osd require-osd-release nautilus

      If this command (step 10 in this procedure) has not been run, OSDs will lose the ability to communicate.

#. For each host that has been upgraded, you should update your
   ``ceph.conf`` file so that it either specifies no monitor port (if
   you are running the monitors on the default ports) or references
   both the v2 and v1 addresses and ports explicitly.  Things will
   still work if only the v1 IP and port are listed, but each CLI
   instantiation or daemon will need to reconnect after learning the
   monitors also speak the v2 protocol, slowing things down a bit and
   preventing a full transition to the v2 protocol.

   This is also a good time to fully transition any config options in
   ``ceph.conf`` into the cluster's configuration database.  On each host,
   you can use the following command to import any options into the
   monitors with::

     ceph config assimilate-conf -i /etc/ceph/ceph.conf

   You can see the cluster's configuration database with::

     ceph config dump
     
   To create a minimal but sufficient ``ceph.conf`` for each host,::

     ceph config generate-minimal-conf > /etc/ceph/ceph.conf.new
     mv /etc/ceph/ceph.conf.new /etc/ceph/ceph.conf

   Be sure to use this new config only on hosts that have been
   upgraded to Nautilus, as it may contain a ``mon_host`` value that
   includes the new ``v2:`` and ``v1:`` prefixes for IP addresses that
   is only understood by Nautilus.

   For more information, see :ref:`msgr2_ceph_conf`.

#. Consider enabling the :ref:`telemetry module <telemetry>` to send
   anonymized usage statistics and crash information to the Ceph
   upstream developers.  To see what would be reported (without actually
   sending any information to anyone),::

     ceph mgr module enable telemetry
     ceph telemetry show

   If you are comfortable with the data that is reported, you can opt-in to
   automatically report the high-level cluster metadata with::

     ceph telemetry on

   For more information about the telemetry module, see :ref:`the
   documentation <telemetry>`.


Upgrading from pre-Luminous releases (like Jewel)
-------------------------------------------------

You *must* first upgrade to Luminous (12.2.z) before attempting an
upgrade to Nautilus.  In addition, your cluster must have completed at
least one scrub of all PGs while running Luminous, setting the
``recovery_deletes`` and ``purged_snapdirs`` flags in the OSD map.


Upgrade compatibility notes
---------------------------

These changes occurred between the Mimic and Nautilus releases.

* ``ceph pg stat`` output has been modified in json
  format to match ``ceph df`` output:

  - "raw_bytes" field renamed to "total_bytes"
  - "raw_bytes_avail" field renamed to "total_bytes_avail"
  - "raw_bytes_avail" field renamed to "total_bytes_avail"
  - "raw_bytes_used" field renamed to "total_bytes_raw_used"
  - "total_bytes_used" field added to represent the space (accumulated over
     all OSDs) allocated purely for data objects kept at block(slow) device
  
* ``ceph df [detail]`` output (GLOBAL section) has been modified in plain
  format:

  - new 'USED' column shows the space (accumulated over all OSDs) allocated
    purely for data objects kept at block(slow) device.
  - 'RAW USED' is now a sum of 'USED' space and space allocated/reserved at
     block device for Ceph purposes, e.g. BlueFS part for BlueStore.

* ``ceph df [detail]`` output (GLOBAL section) has been modified in json
  format:
  
  - 'total_used_bytes' column now shows the space (accumulated over all OSDs)
    allocated purely for data objects kept at block(slow) device
  - new 'total_used_raw_bytes' column shows a sum of 'USED' space and space
    allocated/reserved at block device for Ceph purposes, e.g. BlueFS part for
    BlueStore.

* ``ceph df [detail]`` output (POOLS section) has been modified in plain
  format:
  
  - 'BYTES USED' column renamed to 'STORED'. Represents amount of data
    stored by the user.
  - 'USED' column now represent amount of space allocated purely for data
    by all OSD nodes in KB.
  - 'QUOTA BYTES', 'QUOTA OBJECTS' aren't showed anymore in non-detailed mode.
  - new column 'USED COMPR' - amount of space allocated for compressed
    data. i.e., compressed data plus all the allocation, replication and erasure
    coding overhead.
  - new column 'UNDER COMPR' - amount of data passed through compression
    (summed over all replicas) and beneficial enough to be stored in a
    compressed form.
  - Some columns reordering

* ``ceph df [detail]`` output (POOLS section) has been modified in json
  format:
  
  - 'bytes used' column renamed to 'stored'. Represents amount of data
    stored by the user.
  - 'raw bytes used' column renamed to "stored_raw". Totals of user data
     over all OSD excluding degraded.
  - new 'bytes_used' column now represent amount of space allocated by 
    all OSD nodes.
  - 'kb_used' column - the same as 'bytes_used' but in KB.
  - new column 'compress_bytes_used' - amount of space allocated for compressed
    data. i.e., compressed data plus all the allocation, replication and erasure
    coding overhead.
  - new column 'compress_under_bytes' amount of data passed through compression
    (summed over all replicas) and beneficial enough to be stored in a
    compressed form.

* ``rados df [detail]`` output (POOLS section) has been modified in plain
  format:
  
  - 'USED' column now shows the space (accumulated over all OSDs) allocated
    purely for data objects kept at block(slow) device.
  - new column 'USED COMPR' - amount of space allocated for compressed
    data. i.e., compressed data plus all the allocation, replication and erasure
    coding overhead.
  - new column 'UNDER COMPR' - amount of data passed through compression
    (summed over all replicas) and beneficial enough to be stored in a
    compressed form.

* ``rados df [detail]`` output (POOLS section) has been modified in json
  format:
  
  - 'size_bytes' and 'size_kb' columns now show the space (accumulated
    over all OSDs) allocated purely for data objects kept at block
    device.
  - new column 'compress_bytes_used' - amount of space allocated for compressed
    data. i.e., compressed data plus all the allocation, replication and erasure
    coding overhead.
  - new column 'compress_under_bytes' amount of data passed through compression
    (summed over all replicas) and beneficial enough to be stored in a
    compressed form.

* ``ceph pg dump`` output (totals section) has been modified in json
  format:
  
  - new 'USED' column shows the space (accumulated over all OSDs) allocated
    purely for data objects kept at block(slow) device.
  - 'USED_RAW' is now a sum of 'USED' space and space allocated/reserved at
    block device for Ceph purposes, e.g. BlueFS part for BlueStore.

* The ``ceph osd rm`` command has been deprecated.  Users should use
  ``ceph osd destroy`` or ``ceph osd purge`` (but after first confirming it is
  safe to do so via the ``ceph osd safe-to-destroy`` command).

* The MDS now supports dropping its cache for the purposes of benchmarking.::

    ceph tell mds.* cache drop <timeout>

  Note that the MDS cache is cooperatively managed by the clients. It is
  necessary for clients to give up capabilities in order for the MDS to fully
  drop its cache. This is accomplished by asking all clients to trim as many
  caps as possible. The timeout argument to the ``cache drop`` command controls
  how long the MDS waits for clients to complete trimming caps. This is optional
  and is 0 by default (no timeout). Keep in mind that clients may still retain
  caps to open files which will prevent the metadata for those files from being
  dropped by both the client and the MDS. (This is an equivalent scenario to
  dropping the Linux page/buffer/inode/dentry caches with some processes pinning
  some inodes/dentries/pages in cache.)

* The ``mon_health_preluminous_compat`` and
  ``mon_health_preluminous_compat_warning`` config options are
  removed, as the related functionality is more than two versions old.
  Any legacy monitoring system expecting Jewel-style health output
  will need to be updated to work with Nautilus.

* Nautilus is not supported on any distros still running upstart so upstart
  specific files and references have been removed.

* The ``ceph pg <pgid> list_missing`` command has been renamed to
  ``ceph pg <pgid> list_unfound`` to better match its behaviour.

* The *rbd-mirror* daemon can now retrieve remote peer cluster configuration
  secrets from the monitor. To use this feature, the rbd-mirror daemon
  CephX user for the local cluster must use the ``profile rbd-mirror`` mon cap.
  The secrets can be set using the ``rbd mirror pool peer add`` and
  ``rbd mirror pool peer set`` actions.

* The 'rbd-mirror' daemon will now run in active/active mode by default, where
  mirrored images are evenly distributed between all active 'rbd-mirror'
  daemons. To revert to active/passive mode, override the
  'rbd_mirror_image_policy_type' config key to 'none'.

* The ``ceph mds deactivate`` is fully obsolete and references to it in the docs
  have been removed or clarified.

* The libcephfs bindings added the ``ceph_select_filesystem`` function
  for use with multiple filesystems.

* The cephfs python bindings now include ``mount_root`` and ``filesystem_name``
  options in the mount() function.

* erasure-code: add experimental *Coupled LAYer (CLAY)* erasure codes
  support. It features less network traffic and disk I/O when performing
  recovery.

* The ``cache drop`` OSD command has been added to drop an OSD's caches:

    - ``ceph tell osd.x cache drop``

* The ``cache status`` OSD command has been added to get the cache stats of an
  OSD:

    - ``ceph tell osd.x cache status``

* The libcephfs added several functions that allow restarted client to destroy
  or reclaim state held by a previous incarnation. These functions are for NFS
  servers.

* The ``ceph`` command line tool now accepts keyword arguments in
  the format ``--arg=value`` or ``--arg value``.

* ``librados::IoCtx::nobjects_begin()`` and
  ``librados::NObjectIterator`` now communicate errors by throwing a
  ``std::system_error`` exception instead of ``std::runtime_error``.

* The callback function passed to ``LibRGWFS.readdir()`` now accepts a ``flags``
  parameter. it will be the last parameter passed to  ``readdir()`` method.

* The ``cephfs-data-scan scan_links`` now automatically repair inotables and
  snaptable.

* Configuration values ``mon_warn_not_scrubbed`` and
  ``mon_warn_not_deep_scrubbed`` have been renamed.  They are now
  ``mon_warn_pg_not_scrubbed_ratio`` and ``mon_warn_pg_not_deep_scrubbed_ratio``
  respectively.  This is to clarify that these warnings are related to
  pg scrubbing and are a ratio of the related interval.  These options
  are now enabled by default.

* The MDS cache trimming is now throttled. Dropping the MDS cache
  via the ``ceph tell mds.<foo> cache drop`` command or large reductions in the
  cache size will no longer cause service unavailability.

* The CephFS MDS behavior with recalling caps has been significantly improved
  to not attempt recalling too many caps at once, leading to instability.
  MDS with a large cache (64GB+) should be more stable.

* MDS now provides a config option ``mds_max_caps_per_client`` (default: 1M) to
  limit the number of caps a client session may hold. Long running client
  sessions with a large number of caps have been a source of instability in the
  MDS when all of these caps need to be processed during certain session
  events. It is recommended to not unnecessarily increase this value.

* The MDS config ``mds_recall_state_timeout`` has been removed. Late
  client recall warnings are now generated based on the number of caps
  the MDS has recalled which have not been released. The new configs
  ``mds_recall_warning_threshold`` (default: 32K) and
  ``mds_recall_warning_decay_rate`` (default: 60s) sets the threshold
  for this warning.

* The Telegraf module for the Manager allows for sending statistics to
  an Telegraf Agent over TCP, UDP or a UNIX Socket. Telegraf can then
  send the statistics to databases like InfluxDB, ElasticSearch, Graphite
  and many more.

* The graylog fields naming the originator of a log event have
  changed: the string-form name is now included (e.g., ``"name":
  "mgr.foo"``), and the rank-form name is now in a nested section
  (e.g., ``"rank": {"type": "mgr", "num": 43243}``).

* If the cluster log is directed at syslog, the entries are now
  prefixed by both the string-form name and the rank-form name (e.g.,
  ``mgr.x mgr.12345 ...`` instead of just ``mgr.12345 ...``).

* The JSON output of the ``ceph osd find`` command has replaced the ``ip``
  field with an ``addrs`` section to reflect that OSDs may bind to
  multiple addresses.

* CephFS clients without the 's' flag in their authentication capability
  string will no longer be able to create/delete snapshots. To allow
  ``client.foo`` to create/delete snapshots in the ``bar`` directory of
  filesystem ``cephfs_a``, use command:

    - ``ceph auth caps client.foo mon 'allow r' osd 'allow rw tag cephfs data=cephfs_a' mds 'allow rw, allow rws path=/bar'``

* The ``osd_heartbeat_addr`` option has been removed as it served no
  (good) purpose: the OSD should always check heartbeats on both the
  public and cluster networks.

* The ``rados`` tool's ``mkpool`` and ``rmpool`` commands have been
  removed because they are redundant; please use the ``ceph osd pool
  create`` and ``ceph osd pool rm`` commands instead.

* The ``auid`` property for cephx users and RADOS pools has been
  removed.  This was an undocumented and partially implemented
  capability that allowed cephx users to map capabilities to RADOS
  pools that they "owned".  Because there are no users we have removed
  this support.  If any cephx capabilities exist in the cluster that
  restrict based on auid then they will no longer parse, and the
  cluster will report a health warning like::

    AUTH_BAD_CAPS 1 auth entities have invalid capabilities
        client.bad osd capability parse failed, stopped at 'allow rwx auid 123' of 'allow rwx auid 123'

  The capability can be adjusted with the ``ceph auth caps``
  command. For example,::

    ceph auth caps client.bad osd 'allow rwx pool foo'

* The ``ceph-kvstore-tool`` ``repair`` command has been renamed
  ``destructive-repair`` since we have discovered it can corrupt an
  otherwise healthy rocksdb database.  It should be used only as a last-ditch
  attempt to recover data from an otherwise corrupted store.


* The default memory utilization for the mons has been increased
  somewhat.  Rocksdb now uses 512 MB of RAM by default, which should
  be sufficient for small to medium-sized clusters; large clusters
  should tune this up.  Also, the ``mon_osd_cache_size`` has been
  increase from 10 OSDMaps to 500, which will translate to an
  additional 500 MB to 1 GB of RAM for large clusters, and much less
  for small clusters.

* The ``mgr/balancer/max_misplaced`` option has been replaced by a new
  global ``target_max_misplaced_ratio`` option that throttles both
  balancer activity and automated adjustments to ``pgp_num`` (normally as a
  result of ``pg_num`` changes).  If you have customized the balancer module
  option, you will need to adjust your config to set the new global option
  or revert to the default of .05 (5%).

* By default, Ceph no longer issues a health warning when there are
  misplaced objects (objects that are fully replicated but not stored
  on the intended OSDs).  You can reenable the old warning by setting
  ``mon_warn_on_misplaced`` to ``true``.

* The ``ceph-create-keys`` tool is now obsolete.  The monitors
  automatically create these keys on their own.  For now the script
  prints a warning message and exits, but it will be removed in the
  next release.  Note that ``ceph-create-keys`` would also write the
  admin and bootstrap keys to /etc/ceph and /var/lib/ceph, but this
  script no longer does that.  Any deployment tools that relied on
  this behavior should instead make use of the ``ceph auth export
  <entity-name>`` command for whichever key(s) they need.

* The ``mon_osd_pool_ec_fast_read`` option has been renamed
  ``osd_pool_default_ec_fast_read`` to be more consistent with other
  ``osd_pool_default_*`` options that affect default values for newly
  created RADOS pools.

* The ``mon addr`` configuration option is now deprecated.  It can
  still be used to specify an address for each monitor in the
  ``ceph.conf`` file, but it only affects cluster creation and
  bootstrapping, and it does not support listing multiple addresses
  (e.g., both a v2 and v1 protocol address).  We strongly recommend
  the option be removed and instead a single ``mon host`` option be
  specified in the ``[global]`` section to allow daemons and clients
  to discover the monitors.

* New command ``ceph fs fail`` has been added to quickly bring down a file
  system. This is a single command that unsets the joinable flag on the file
  system and brings down all of its ranks.

* The ``cache drop`` admin socket command has been removed. The ``ceph
  tell mds.X cache drop`` remains.


Detailed Changelog
------------------
* add monitoring subdir and Grafana cluster dashboard (`pr#21850 <https://github.com/ceph/ceph/pull/21850>`_, Jan Fajerski)
* auth,common: include cleanups (`pr#23774 <https://github.com/ceph/ceph/pull/23774>`_, Kefu Chai)
* bluestore: bluestore/NVMEDevice.cc: fix ceph_assert() when enable SPDK with 64KB kernel page size (`issue#36624 <http://tracker.ceph.com/issues/36624>`_, `pr#24817 <https://github.com/ceph/ceph/pull/24817>`_, tone.zhang)
* bluestore: bluestore/NVMEDevice.cc: fix NVMEManager thread hang (`issue#37720 <http://tracker.ceph.com/issues/37720>`_, `pr#25646 <https://github.com/ceph/ceph/pull/25646>`_, tone.zhang, Steve Capper)
* bluestore: bluestore/NVMe: use PCIe selector as the path name (`pr#24144 <https://github.com/ceph/ceph/pull/24144>`_, Kefu Chai)
* bluestore,cephfs,core,rbd,rgw: buffer,denc: use ptr::const_iterator for decode (`pr#22015 <https://github.com/ceph/ceph/pull/22015>`_, Kefu Chai, Casey Bodley)
* bluestore: ceph-kvstore-tool: dump fixes (`pr#25262 <https://github.com/ceph/ceph/pull/25262>`_, Adam Kupczyk)
* bluestore: common/blkdev: check retval of stat() (`pr#26040 <https://github.com/ceph/ceph/pull/26040>`_, Kefu Chai)
* bluestore,core: ceph-dencoder: add bluefs types (`pr#22463 <https://github.com/ceph/ceph/pull/22463>`_, Sage Weil)
* bluestore,core,mon,performance: osd,mon: enable level_compaction_dynamic_level_bytes for rocksdb (`issue#24361 <http://tracker.ceph.com/issues/24361>`_, `pr#22337 <https://github.com/ceph/ceph/pull/22337>`_, Kefu Chai)
* bluestore,core: os/bluestore: don't store/use path_block.{db,wal} from meta (`pr#22462 <https://github.com/ceph/ceph/pull/22462>`_, Sage Weil, Alfredo Deza)
* bluestore: os/bluestore: add bluestore_ignore_data_csum option (`pr#26233 <https://github.com/ceph/ceph/pull/26233>`_, Sage Weil)
* bluestore: os/bluestore: add boundary check for cache-autotune related settings (`issue#37507 <http://tracker.ceph.com/issues/37507>`_, `pr#25421 <https://github.com/ceph/ceph/pull/25421>`_, xie xingguo)
* bluestore: os/bluestore/BlueFS: only flush dirty devices when do _fsync (`pr#22110 <https://github.com/ceph/ceph/pull/22110>`_, Jianpeng Ma)
* bluestore: os/bluestore: bluestore_buffer_hit_bytes perf counter doesn't reset (`pr#23576 <https://github.com/ceph/ceph/pull/23576>`_, Igor Fedotov)
* bluestore: os/bluestore: check return value of _open_bluefs (`pr#25471 <https://github.com/ceph/ceph/pull/25471>`_, Jianpeng Ma)
* bluestore: os/bluestore: cleanups (`pr#22556 <https://github.com/ceph/ceph/pull/22556>`_, Jianpeng Ma)
* bluestore: os/bluestore: deep fsck fails on inspecting very large onodes (`pr#26170 <https://github.com/ceph/ceph/pull/26170>`_, Igor Fedotov)
* bluestore: os/bluestore: do not assert on non-zero err codes from  compress() call (`pr#25891 <https://github.com/ceph/ceph/pull/25891>`_, Igor Fedotov)
* bluestore: os/bluestore: firstly delete db then delete bluefs if open db met error (`pr#22336 <https://github.com/ceph/ceph/pull/22336>`_, Jianpeng Ma)
* bluestore: os/bluestore: fix and unify log output on allocation failure (`pr#25335 <https://github.com/ceph/ceph/pull/25335>`_, Igor Fedotov)
* bluestore: os/bluestore: fix assertion in StupidAllocator::get_fragmentation (`pr#23606 <https://github.com/ceph/ceph/pull/23606>`_, Igor Fedotov)
* bluestore: os/bluestore: fix bloom filter num entry miscalculation in repairer (`issue#25001 <http://tracker.ceph.com/issues/25001>`_, `pr#24076 <https://github.com/ceph/ceph/pull/24076>`_, Igor Fedotov)
* bluestore: os/bluestore: fix bluefs extent miscalculations on small slow device (`pr#22563 <https://github.com/ceph/ceph/pull/22563>`_, Igor Fedotov)
* bluestore: os/bluestore: fix race between remove_collection and object removals (`pr#23257 <https://github.com/ceph/ceph/pull/23257>`_, Igor Fedotov)
* bluestore: os/bluestore: fixup access a destroy cond cause deadlock or undefine behavior (`pr#25659 <https://github.com/ceph/ceph/pull/25659>`_, linbing)
* bluestore: os/bluestore: introduce new BlueFS perf counter to track the amount of (`pr#22086 <https://github.com/ceph/ceph/pull/22086>`_, Igor Fedotov)
* bluestore: os/bluestore/KernelDevice: misc cleanup (`pr#21491 <https://github.com/ceph/ceph/pull/21491>`_, Jianpeng Ma)
* bluestore: os/bluestore/KernelDevice: use flock(2) for block device lock (`issue#38150 <http://tracker.ceph.com/issues/38150>`_, `pr#26245 <https://github.com/ceph/ceph/pull/26245>`_, Sage Weil)
* bluestore: os/bluestore: misc cleanup (`pr#22472 <https://github.com/ceph/ceph/pull/22472>`_, Jianpeng Ma)
* bluestore: os/bluestore: Only use F_SET_FILE_RW_HINT when available (`pr#26431 <https://github.com/ceph/ceph/pull/26431>`_, Willem Jan Withagen)
* bluestore: os/bluestore: Only use ``WRITE_LIFE_`` when available (`pr#25735 <https://github.com/ceph/ceph/pull/25735>`_, Willem Jan Withagen)
* bluestore: os/bluestore: remove redundant fault_range (`pr#22898 <https://github.com/ceph/ceph/pull/22898>`_, Jianpeng Ma)
* bluestore: os/bluestore: remove useless condtion (`pr#22335 <https://github.com/ceph/ceph/pull/22335>`_, Jianpeng Ma)
* bluestore: os/bluestore: simplify and fix SharedBlob::put() (`issue#24211 <http://tracker.ceph.com/issues/24211>`_, `pr#22123 <https://github.com/ceph/ceph/pull/22123>`_, Sage Weil)
* bluestore: os/bluestore: support for FreeBSD (`pr#25608 <https://github.com/ceph/ceph/pull/25608>`_, Alan Somers, Kefu Chai)
* bluestore: osd/osd_types: fix pg_t::contains() to check pool id too (`issue#32731 <http://tracker.ceph.com/issues/32731>`_, `pr#24085 <https://github.com/ceph/ceph/pull/24085>`_, Sage Weil)
* bluestore: os/objectstore: add a new op OP_CREATE (`pr#22385 <https://github.com/ceph/ceph/pull/22385>`_, Jianpeng Ma)
* bluestore,performance: common/PriorityCache: First Step toward priority based caching (`pr#22009 <https://github.com/ceph/ceph/pull/22009>`_, Mark Nelson)
* bluestore,performance: os/bluestore: allocator pruning (`pr#21854 <https://github.com/ceph/ceph/pull/21854>`_, Igor Fedotov)
* bluestore,performance: os/bluestore/BlueFS: reduce bufferlist rebuilds during WAL writes (`pr#21689 <https://github.com/ceph/ceph/pull/21689>`_, Piotr Dałek)
* bluestore,performance: os/bluestore: use the monotonic clock for perf counters latencies (`pr#22121 <https://github.com/ceph/ceph/pull/22121>`_, Mohamad Gebai)
* bluestore: silence Clang warning on possible uninitialize usuage (`pr#25702 <https://github.com/ceph/ceph/pull/25702>`_, Willem Jan Withagen)
* bluestore: spdk: fix ceph-osd crash when activate SPDK (`issue#24371 <http://tracker.ceph.com/issues/24371>`_, `pr#22356 <https://github.com/ceph/ceph/pull/22356>`_, tone-zhang)
* bluestore: test/fio: add option single_pool_mode in ceph-bluestore.fio (`pr#21929 <https://github.com/ceph/ceph/pull/21929>`_, Jianpeng Ma)
* bluestore,tests: test/objectstore: fix random generator in allocator_bench (`pr#22544 <https://github.com/ceph/ceph/pull/22544>`_, Igor Fedotov)
* bluestore,tools: os/bluestore: allow ceph-bluestore-tool to coalesce, add and migrate BlueFS backing volumes (`pr#23103 <https://github.com/ceph/ceph/pull/23103>`_, Igor Fedotov)
* bluestore,tools: tools/ceph-bluestore-tool: avoid mon/config access when calling global… (`pr#22085 <https://github.com/ceph/ceph/pull/22085>`_, Igor Fedotov)
* build/ops: Add new OpenSUSE Leap id for install-deps.sh (`issue#25064 <http://tracker.ceph.com/issues/25064>`_, `pr#22793 <https://github.com/ceph/ceph/pull/22793>`_, Kyr Shatskyy)
* build/ops: arch/arm: Allow ceph_crc32c_aarch64 to be chosen only if it is compil… (`pr#24126 <https://github.com/ceph/ceph/pull/24126>`_, David Wang)
* build/ops:  auth: do not use GSS/KRB5 if ! HAVE_GSSAPI (`pr#25460 <https://github.com/ceph/ceph/pull/25460>`_, Kefu Chai)
* build/ops: build: 32 bit architecture fixes (`pr#23485 <https://github.com/ceph/ceph/pull/23485>`_, James Page)
* build/ops: build: further removal of `subman` configuration (`issue#38261 <http://tracker.ceph.com/issues/38261>`_, `pr#26368 <https://github.com/ceph/ceph/pull/26368>`_, Alfredo Deza)
* build/ops: build: LLVM ld does not like the versioning scheme (`pr#26801 <https://github.com/ceph/ceph/pull/26801>`_, Willem Jan Withagen)
* build/ops: ceph-create-keys: Misc Python 3 fixes (`issue#37641 <http://tracker.ceph.com/issues/37641>`_, `pr#25411 <https://github.com/ceph/ceph/pull/25411>`_, James Page)
* build/ops,cephfs: deb,rpm: fix python-cephfs dependencies (`issue#24919 <http://tracker.ceph.com/issues/24919>`_, `issue#24918 <http://tracker.ceph.com/issues/24918>`_, `pr#23043 <https://github.com/ceph/ceph/pull/23043>`_, Kefu Chai)
* build/ops: ceph.in: Add support for python 3 (`pr#24739 <https://github.com/ceph/ceph/pull/24739>`_, Tiago Melo)
* build/ops: ceph.spec.in: Don't use noarch for mgr module subpackages, fix /usr/lib64/ceph/mgr dir ownership (`pr#26398 <https://github.com/ceph/ceph/pull/26398>`_, Tim Serong)
* build/ops: change ceph-mgr package depency from py-bcrypt to python2-bcrypt (`issue#27206 <http://tracker.ceph.com/issues/27206>`_, `pr#23648 <https://github.com/ceph/ceph/pull/23648>`_, Konstantin Sakhinov)
* build/ops: civetweb: pull up to ceph-master (`pr#26515 <https://github.com/ceph/ceph/pull/26515>`_, Abhishek Lekshmanan)
* build/ops: cmake,do_freebsd.sh: disable rdma features (`pr#22752 <https://github.com/ceph/ceph/pull/22752>`_, Kefu Chai)
* build/ops: cmake/modules/BuildDPDK.cmake: Build required DPDK libraries (`issue#36341 <http://tracker.ceph.com/issues/36341>`_, `pr#24487 <https://github.com/ceph/ceph/pull/24487>`_, Brad Hubbard)
* build/ops: cmake/modules/BuildRocksDB.cmake: enable compressions for rocksdb (`issue#24025 <http://tracker.ceph.com/issues/24025>`_, `pr#22181 <https://github.com/ceph/ceph/pull/22181>`_, Kefu Chai)
* build/ops: cmake,rgw: make amqp support optional (`pr#26555 <https://github.com/ceph/ceph/pull/26555>`_, Kefu Chai)
* build/ops: cmake,rpm,deb: install mgr plugins into /usr/share/ceph/mgr (`pr#26446 <https://github.com/ceph/ceph/pull/26446>`_, Kefu Chai)
* build/ops: cmake,seastar: pick up latest seastar (`pr#25474 <https://github.com/ceph/ceph/pull/25474>`_, Kefu Chai)
* build/ops,common: compressor: Fix build of Brotli Compressor (`pr#24967 <https://github.com/ceph/ceph/pull/24967>`_, BI SHUN KE)
* build/ops,common,core: test: make readable.sh fail if it doesn't run anything (`pr#24812 <https://github.com/ceph/ceph/pull/24812>`_, Greg Farnum)
* build/ops,core: cmake,common,filestore: silence gcc-8 warnings/errors (`pr#21837 <https://github.com/ceph/ceph/pull/21837>`_, Kefu Chai)
* build/ops,core,rbd: include/memory.h: remove memory.h (`pr#22690 <https://github.com/ceph/ceph/pull/22690>`_, Kefu Chai)
* build/ops,core: systemd: only restart 3 times in 30 minutes, as fast as possible (`issue#24368 <http://tracker.ceph.com/issues/24368>`_, `pr#22349 <https://github.com/ceph/ceph/pull/22349>`_, Greg Farnum)
* build/ops,core,tests: objectstore/test/fio: Fixed fio compilation when tcmalloc is used (`pr#23962 <https://github.com/ceph/ceph/pull/23962>`_, Adam Kupczyk)
* build/ops: credits.sh: Ignore package-lock.json and .xlf files (`pr#24762 <https://github.com/ceph/ceph/pull/24762>`_, Tiago Melo)
* build/ops: deb: drop redundant ceph-common recommends (`pr#20133 <https://github.com/ceph/ceph/pull/20133>`_, Nathan Cutler)
* build/ops: debian/control: change Architecture python plugins to "all" (`pr#26377 <https://github.com/ceph/ceph/pull/26377>`_, Kefu Chai)
* build/ops: debian/control: require fuse for ceph-fuse (`issue#21057 <http://tracker.ceph.com/issues/21057>`_, `pr#23675 <https://github.com/ceph/ceph/pull/23675>`_, Thomas Serlin)
* build/ops: debian: correct ceph-common relationship with older radosgw package (`pr#24996 <https://github.com/ceph/ceph/pull/24996>`_, Matthew Vernon)
* build/ops: debian: drop '-DUSE_CRYPTOPP=OFF' from cmake options (`pr#22471 <https://github.com/ceph/ceph/pull/22471>`_, Kefu Chai)
* build/ops: debian: librados-dev should replace librados2-dev (`pr#25916 <https://github.com/ceph/ceph/pull/25916>`_, Kefu Chai)
* build/ops: debian/rules: fix ceph-mgr .pyc files left behind (`issue#26883 <http://tracker.ceph.com/issues/26883>`_, `pr#23615 <https://github.com/ceph/ceph/pull/23615>`_, Dan Mick)
* build/ops: deb,rpm,do_cmake: switch to cmake3 (`pr#22896 <https://github.com/ceph/ceph/pull/22896>`_, Kefu Chai)
* build/ops: dmclock, cmake: sync up with ceph/dmclock, dmclock related cleanups (`issue#26998 <http://tracker.ceph.com/issues/26998>`_, `pr#23643 <https://github.com/ceph/ceph/pull/23643>`_, Kefu Chai)
* build/ops: dmclock: update dmclock submodule sha1 to tip of ceph/dmclock.git master (`pr#23837 <https://github.com/ceph/ceph/pull/23837>`_, Ricardo Dias)
* build/ops: do_cmake.sh: automate py3 build options for certain distros (`pr#25205 <https://github.com/ceph/ceph/pull/25205>`_, Nathan Cutler)
* build/ops: do_cmake.sh: SUSE builds need WITH_RADOSGW_AMQP_ENDPOINT=OFF (`pr#26695 <https://github.com/ceph/ceph/pull/26695>`_, Nathan Cutler)
* build/ops: do_freebsd.sh: FreeBSD building needs the llvm linker (`pr#25247 <https://github.com/ceph/ceph/pull/25247>`_, Willem Jan Withagen)
* build/ops: dout: declare dpp using `decltype(auto)` instead of `auto` (`pr#22207 <https://github.com/ceph/ceph/pull/22207>`_, Kefu Chai)
* build/ops: dpdk: drop dpdk submodule (`issue#24032 <http://tracker.ceph.com/issues/24032>`_, `pr#21856 <https://github.com/ceph/ceph/pull/21856>`_, Kefu Chai)
* build/ops: examples/Makefile: add -Wno-unused-parameter to avoid compile error (`pr#23581 <https://github.com/ceph/ceph/pull/23581>`_, You Ji)
* build/ops: Improving make check reliability (`pr#22441 <https://github.com/ceph/ceph/pull/22441>`_, Erwan Velu)
* build/ops: include: define errnos if not defined for better portablity (`pr#25302 <https://github.com/ceph/ceph/pull/25302>`_, Willem Jan Withagen)
* build/ops: install-deps: check the exit status for the $builddepcmd (`pr#22682 <https://github.com/ceph/ceph/pull/22682>`_, Yunchuan Wen)
* build/ops: install-deps: do not specify unknown options (`pr#24315 <https://github.com/ceph/ceph/pull/24315>`_, Kefu Chai)
* build/ops: install-deps: install setuptools before upgrading virtualenv (`pr#25039 <https://github.com/ceph/ceph/pull/25039>`_, Kefu Chai)
* build/ops: install-deps: nuke wheelhouse if it's stale (`pr#22028 <https://github.com/ceph/ceph/pull/22028>`_, Kefu Chai)
* build/ops: install-deps,run-make-check: use ceph-libboost repo (`issue#25186 <http://tracker.ceph.com/issues/25186>`_, `pr#23995 <https://github.com/ceph/ceph/pull/23995>`_, Kefu Chai)
* build/ops: install-deps.sh: Add Kerberos requirement for FreeBSD (`pr#25688 <https://github.com/ceph/ceph/pull/25688>`_, Willem Jan Withagen)
* build/ops: install-deps.sh: disable centos-sclo-rh-source (`issue#37707 <http://tracker.ceph.com/issues/37707>`_, `pr#25629 <https://github.com/ceph/ceph/pull/25629>`_, Brad Hubbard)
* build/ops: install-deps.sh: fix gcc detection and install pre-built libboost on bionic (`pr#25169 <https://github.com/ceph/ceph/pull/25169>`_, Changcheng Liu, Kefu Chai)
* build/ops: install-deps.sh: fix installing gcc on ubuntu when no old compiler (`pr#22488 <https://github.com/ceph/ceph/pull/22488>`_, Tomasz Setkowski)
* build/ops: install-deps.sh: import ubuntu-toolchain-r's key without keyserver (`pr#22964 <https://github.com/ceph/ceph/pull/22964>`_, Kefu Chai)
* build/ops: install-deps.sh: install libtool-ltdl-devel for building python-saml (`pr#25071 <https://github.com/ceph/ceph/pull/25071>`_, Kefu Chai)
* build/ops: install-deps.sh: refrain from installing/using lsb_release, and other cleanup (`issue#18163 <http://tracker.ceph.com/issues/18163>`_, `pr#23361 <https://github.com/ceph/ceph/pull/23361>`_, Nathan Cutler)
* build/ops: install-deps.sh: Remove CR repo (`issue#13997 <http://tracker.ceph.com/issues/13997>`_, `pr#25211 <https://github.com/ceph/ceph/pull/25211>`_, Brad Hubbard, Alfredo Deza)
* build/ops:  install-deps.sh: selectively install dependencies (`pr#26402 <https://github.com/ceph/ceph/pull/26402>`_, Kefu Chai)
* build/ops: install-deps.sh: set with_seastar (`pr#23079 <https://github.com/ceph/ceph/pull/23079>`_, Nathan Cutler)
* build/ops: install-deps.sh: support install gcc7 in xenial aarch64 (`pr#22451 <https://github.com/ceph/ceph/pull/22451>`_, Yunchuan Wen)
* build/ops: install-deps.sh: Update python requirements for FreeBSD (`pr#25245 <https://github.com/ceph/ceph/pull/25245>`_, Willem Jan Withagen)
* build/ops: install-deps.sh: use the latest setuptools (`pr#26156 <https://github.com/ceph/ceph/pull/26156>`_, Kefu Chai)
* build/ops: install-deps: s/openldap-client/openldap24-client/ (`pr#23912 <https://github.com/ceph/ceph/pull/23912>`_, Kefu Chai)
* build/ops: libradosstriper: conditional compile (`pr#21983 <https://github.com/ceph/ceph/pull/21983>`_, Jesse Williamson)
* build/ops: make-debs.sh: clean dir to allow building deb packages multiple times (`pr#25177 <https://github.com/ceph/ceph/pull/25177>`_, Changcheng Liu)
* build/ops: man: skip directive starting with ".." (`pr#23580 <https://github.com/ceph/ceph/pull/23580>`_, Kefu Chai)
* build/ops,mgr: build: mgr: check for python's ssl version linkage (`issue#24282 <http://tracker.ceph.com/issues/24282>`_, `pr#22659 <https://github.com/ceph/ceph/pull/22659>`_, Kefu Chai, Abhishek Lekshmanan)
* build/ops,mgr: cmake,deb,rpm: remove cython 0.29's subinterpreter check, re-enable build with cython 0.29+ (`pr#25585 <https://github.com/ceph/ceph/pull/25585>`_, Tim Serong)
* build/ops: mgr/dashboard: Add html-linter (`pr#24273 <https://github.com/ceph/ceph/pull/24273>`_, Tiago Melo)
* build/ops: mgr/dashboard: Add i18n validation script (`pr#25179 <https://github.com/ceph/ceph/pull/25179>`_, Tiago Melo)
* build/ops: mgr/dashboard: Add package-lock.json (`pr#23285 <https://github.com/ceph/ceph/pull/23285>`_, Tiago Melo)
* build/ops: mgr/dashboard: Disable showing xi18n's progress (`pr#25427 <https://github.com/ceph/ceph/pull/25427>`_, Tiago Melo)
* build/ops: mgr/dashboard: Fix run-frontend-e2e-tests.sh (`pr#25157 <https://github.com/ceph/ceph/pull/25157>`_, Tiago Melo)
* build/ops: mgr/dashboard: fix the version of all frontend dependencies (`pr#22712 <https://github.com/ceph/ceph/pull/22712>`_, Tiago Melo)
* build/ops: mgr/dashboard: Remove angular build progress logs during cmake (`pr#23115 <https://github.com/ceph/ceph/pull/23115>`_, Tiago Melo)
* build/ops: mgr/dashboard: Update Node.js to current LTS (`pr#24932 <https://github.com/ceph/ceph/pull/24932>`_, Tiago Melo)
* build/ops: mgr/dashboard: Update node version (`pr#22639 <https://github.com/ceph/ceph/pull/22639>`_, Tiago Melo)
* build/ops: mgr/diskprediction: Replace local predictor model file (`pr#24484 <https://github.com/ceph/ceph/pull/24484>`_, Rick Chen)
* build/ops,mgr: mgr/dashboard: Fix building under FreeBSD (`pr#22562 <https://github.com/ceph/ceph/pull/22562>`_, Willem Jan Withagen)
* build/ops: move dmclock subtree into submodule (`pr#21651 <https://github.com/ceph/ceph/pull/21651>`_, Danny Al-Gaaf)
* build/ops,pybind: ceph: do not raise StopIteration within generator (`pr#25400 <https://github.com/ceph/ceph/pull/25400>`_, Jason Dillaman)
* build/ops,rbd: osd,mon,pybind: Make able to compile with Clang (`pr#21861 <https://github.com/ceph/ceph/pull/21861>`_, Adam C. Emerson)
* build/ops,rbd: selinux: add support for ceph iscsi (`pr#24936 <https://github.com/ceph/ceph/pull/24936>`_, Mike Christie)
* build/ops,rbd: systemd: enable ceph-rbd-mirror.target (`pr#24935 <https://github.com/ceph/ceph/pull/24935>`_, Sébastien Han)
* build/ops,rgw: build/rgw: unittest_rgw_dmclock_scheduler does not need Boost_LIBRARIES (`pr#26799 <https://github.com/ceph/ceph/pull/26799>`_, Willem Jan Withagen)
* build/ops,rgw: cls: build cls_otp only WITH_RADOSGW (`pr#22548 <https://github.com/ceph/ceph/pull/22548>`_, Piotr Dałek)
* build/ops,rgw: deb,rpm: package librgw_admin_user.{h,so.\*} (`pr#22205 <https://github.com/ceph/ceph/pull/22205>`_, Kefu Chai)
* build/ops: rocksdb: sync with upstream (`issue#23653 <http://tracker.ceph.com/issues/23653>`_, `pr#22236 <https://github.com/ceph/ceph/pull/22236>`_, Kefu Chai)
* build/ops: rpm: bump up required GCC version to 7.3.1 (`pr#24130 <https://github.com/ceph/ceph/pull/24130>`_, Kefu Chai)
* build/ops: rpm,deb: remove python-jinja2 dependency (`pr#26379 <https://github.com/ceph/ceph/pull/26379>`_, Kefu Chai)
* build/ops: rpm: do not exclude s390x build on openSUSE (`pr#26268 <https://github.com/ceph/ceph/pull/26268>`_, Nathan Cutler)
* build/ops: rpm: Fix Fedora error "No matching package to install: 'Cython3'" (`issue#35831 <http://tracker.ceph.com/issues/35831>`_, `pr#23993 <https://github.com/ceph/ceph/pull/23993>`_, Brad Hubbard)
* build/ops: rpm: fix libradospp-devel runtime dependency (`pr#25491 <https://github.com/ceph/ceph/pull/25491>`_, Nathan Cutler)
* build/ops: rpm: fix seastar build dependencies for SUSE (`pr#23089 <https://github.com/ceph/ceph/pull/23089>`_, Nathan Cutler)
* build/ops: rpm: fix seastar build dependencies (`pr#23386 <https://github.com/ceph/ceph/pull/23386>`_, Nathan Cutler)
* build/ops: rpm: fix xmlsec1 build dependency for dashboard make check (`pr#26119 <https://github.com/ceph/ceph/pull/26119>`_, Nathan Cutler)
* build/ops: rpm: Install python2-Cython on f28 (`pr#26756 <https://github.com/ceph/ceph/pull/26756>`_, Brad Hubbard)
* build/ops: rpm: make ceph-grafana-dashboards own its directories (`issue#37485 <http://tracker.ceph.com/issues/37485>`_, `pr#25347 <https://github.com/ceph/ceph/pull/25347>`_, Nathan Cutler, Tim Serong)
* build/ops: rpm: make Python dependencies somewhat less confusing (`pr#25963 <https://github.com/ceph/ceph/pull/25963>`_, Nathan Cutler)
* build/ops: rpm: make sudo a build dependency (`pr#23077 <https://github.com/ceph/ceph/pull/23077>`_, Nathan Cutler)
* build/ops: rpm: package crypto libraries for all archs (`pr#26202 <https://github.com/ceph/ceph/pull/26202>`_, Nathan Cutler)
* build/ops: rpm: Package grafana dashboards (`pr#24735 <https://github.com/ceph/ceph/pull/24735>`_, Boris Ranto)
* build/ops: rpm: provide files moved from ceph-test … (`issue#22558 <http://tracker.ceph.com/issues/22558>`_, `pr#20401 <https://github.com/ceph/ceph/pull/20401>`_, Nathan Cutler)
* build/ops: rpm: RHEL 8 fixes (`pr#26520 <https://github.com/ceph/ceph/pull/26520>`_, Ken Dreyer)
* build/ops: rpm: RHEL 8 needs Python 3 build (`pr#25223 <https://github.com/ceph/ceph/pull/25223>`_, Nathan Cutler)
* build/ops: rpm: stop install-deps.sh clobbering spec file Python build setting (`issue#37301 <http://tracker.ceph.com/issues/37301>`_, `pr#25181 <https://github.com/ceph/ceph/pull/25181>`_, Nathan Cutler, Brad Hubbard)
* build/ops: rpm: Use hardened LDFLAGS (`issue#36316 <http://tracker.ceph.com/issues/36316>`_, `pr#24425 <https://github.com/ceph/ceph/pull/24425>`_, Boris Ranto)
* build/ops: rpm: use updated gperftools (`issue#35969 <http://tracker.ceph.com/issues/35969>`_, `pr#24124 <https://github.com/ceph/ceph/pull/24124>`_, Kefu Chai)
* build/ops: rpm: Use updated gperftools-libs at runtime (`issue#36508 <http://tracker.ceph.com/issues/36508>`_, `pr#24652 <https://github.com/ceph/ceph/pull/24652>`_, Brad Hubbard)
* build/ops: run-make-check: enable --with-seastar option (`pr#22809 <https://github.com/ceph/ceph/pull/22809>`_, Kefu Chai)
* build/ops: run-make-check: set WITH_SEASTAR with a non-empty string (`pr#23108 <https://github.com/ceph/ceph/pull/23108>`_, Kefu Chai)
* build/ops: run-make-check.sh: Adding ccache tuning for the CI (`pr#22847 <https://github.com/ceph/ceph/pull/22847>`_, Erwan Velu)
* build/ops: run-make-check.sh: ccache goodness for everyone (`issue#24817 <http://tracker.ceph.com/issues/24817>`_, `issue#24777 <http://tracker.ceph.com/issues/24777>`_, `pr#22867 <https://github.com/ceph/ceph/pull/22867>`_, Nathan Cutler)
* build/ops: run-make-check: should use sudo for running sysctl (`pr#23708 <https://github.com/ceph/ceph/pull/23708>`_, Kefu Chai)
* build/ops: run-make-check: Showing configuration before the build (`pr#23609 <https://github.com/ceph/ceph/pull/23609>`_, Erwan Velu)
* build/ops: seastar: lower the required yaml-cpp version to 0.5.1 (`pr#23255 <https://github.com/ceph/ceph/pull/23255>`_, Kefu Chai)
* build/ops: seastar: pickup the change to link pthread (`pr#25671 <https://github.com/ceph/ceph/pull/25671>`_, Kefu Chai)
* build/ops: selinux: Allow ceph to execute ldconfig (`pr#20118 <https://github.com/ceph/ceph/pull/20118>`_, Boris Ranto)
* build/ops: spdk: update to latest spdk-18.05 branch (`pr#22547 <https://github.com/ceph/ceph/pull/22547>`_, Kefu Chai)
* build/ops: spec: requires ceph base instead of common (`issue#37620 <http://tracker.ceph.com/issues/37620>`_, `pr#25503 <https://github.com/ceph/ceph/pull/25503>`_, Sébastien Han)
* build/ops: test: move ceph-dencoder to src/tools (`pr#23228 <https://github.com/ceph/ceph/pull/23228>`_, Kefu Chai)
* build/ops: test,qa: s/.libs/lib/ (`pr#20734 <https://github.com/ceph/ceph/pull/20734>`_, Kefu Chai)
* build/ops,tests: cmake,run-make-check: always enable WITH_GTEST_PARALLEL (`pr#23382 <https://github.com/ceph/ceph/pull/23382>`_, Kefu Chai)
* build/ops,tests: deb,rpm,qa: split dashboard package (`pr#26380 <https://github.com/ceph/ceph/pull/26380>`_, Kefu Chai)
* build/ops,tests: mgr/dashboard: Fix localStorage problem in Jest (`pr#23281 <https://github.com/ceph/ceph/pull/23281>`_, Tiago Melo)
* build/ops,tests: mgr/dashboard: Object Gateway user configuration (`pr#25494 <https://github.com/ceph/ceph/pull/25494>`_, Laura Paduano)
* build/ops,tests: src/test: Using gtest-parallel to speedup unittests (`pr#22577 <https://github.com/ceph/ceph/pull/22577>`_, Kefu Chai, Erwan Velu)
* build/ops,tests: tests/fio: fix build failures and ensure this is covered by run-make-check.sh (`pr#23231 <https://github.com/ceph/ceph/pull/23231>`_, Kefu Chai, Igor Fedotov)
* build/ops,tests: tests/qa: Adding $ distro mix - rgw (`pr#21932 <https://github.com/ceph/ceph/pull/21932>`_, Yuri Weinstein)
* build/ops,tests: tools/ceph-dencoder: conditionally link against mds (`pr#25255 <https://github.com/ceph/ceph/pull/25255>`_, Kefu Chai)
* build/ops,tools: tool: link rbd-ggate against librados-cxx (`pr#24901 <https://github.com/ceph/ceph/pull/24901>`_, Willem Jan Withagen)
* ceph-disk: get_partition_dev() should fail until get_dev_path(partnam… (`pr#21415 <https://github.com/ceph/ceph/pull/21415>`_, Erwan Velu)
* cephfs: doc/releases: update CephFS mimic notes (`issue#23775 <http://tracker.ceph.com/issues/23775>`_, `pr#22232 <https://github.com/ceph/ceph/pull/22232>`_, Patrick Donnelly)
* cephfs: mgr/dashboard: NFS Ganesha management REST API (`pr#25918 <https://github.com/ceph/ceph/pull/25918>`_, Lenz Grimmer, Ricardo Dias, Jeff Layton)
* cephfs,mgr,pybind: pybind/mgr: Unified bits of volumes and orchestrator (`pr#25492 <https://github.com/ceph/ceph/pull/25492>`_, Sebastian Wagner)
* cephfs,mon: MDSMonitor: silence unable to load metadata (`pr#25693 <https://github.com/ceph/ceph/pull/25693>`_, Song Shun)
* cephfs,mon: mon/MDSMonitor: do not send redundant MDS health messages to cluster log (`issue#24308 <http://tracker.ceph.com/issues/24308>`_, `pr#22252 <https://github.com/ceph/ceph/pull/22252>`_, Sage Weil)
* cephfs: qa: fix symlink (`pr#23997 <https://github.com/ceph/ceph/pull/23997>`_, Patrick Donnelly)
* cephfs,rbd: osdc: Fix the wrong BufferHead offset (`pr#22495 <https://github.com/ceph/ceph/pull/22495>`_, dongdong tao)
* cephfs,rbd: osdc: optimize the code doing the BufferHead mapping (`pr#22509 <https://github.com/ceph/ceph/pull/22509>`_, dongdong tao)
* cephfs,rbd: osdc: reduce ObjectCacher's memory fragments (`issue#36192 <http://tracker.ceph.com/issues/36192>`_, `pr#24297 <https://github.com/ceph/ceph/pull/24297>`_, "Yan, Zheng")
* cephfs,tests: qa: fix run call args (`issue#36450 <http://tracker.ceph.com/issues/36450>`_, `pr#24597 <https://github.com/ceph/ceph/pull/24597>`_, Patrick Donnelly)
* cephfs,tests: qa: install python3-cephfs for fs suite (`pr#23411 <https://github.com/ceph/ceph/pull/23411>`_, Kefu Chai)
* cephfs,tests: qa/suites/powercycle: whitelist MDS_SLOW_REQUEST (`pr#23151 <https://github.com/ceph/ceph/pull/23151>`_, Neha Ojha)
* cephfs,tests: qa/workunits/suites/pjd.sh: use correct dir name (`pr#22233 <https://github.com/ceph/ceph/pull/22233>`_, Neha Ojha)
* ceph-volume:  activate option --auto-detect-objectstore respects --no-systemd (`issue#36249 <http://tracker.ceph.com/issues/36249>`_, `pr#24355 <https://github.com/ceph/ceph/pull/24355>`_, Alfredo Deza)
* ceph-volume: Adapt code to support Python3 (`pr#25324 <https://github.com/ceph/ceph/pull/25324>`_, Volker Theile)
* ceph-volume: add --all flag to simple activate (`pr#26225 <https://github.com/ceph/ceph/pull/26225>`_, Jan Fajerski)
* ceph-volume add a __release__ string, to help version-conditional calls (`issue#25171 <http://tracker.ceph.com/issues/25171>`_, `pr#23332 <https://github.com/ceph/ceph/pull/23332>`_, Alfredo Deza)
* ceph-volume: add inventory command (`issue#24972 <http://tracker.ceph.com/issues/24972>`_, `pr#24859 <https://github.com/ceph/ceph/pull/24859>`_, Jan Fajerski)
* ceph-volume: Additional work on ceph-volume to add some choose_disk capabilities (`issue#36446 <http://tracker.ceph.com/issues/36446>`_, `pr#24504 <https://github.com/ceph/ceph/pull/24504>`_, Erwan Velu)
* ceph-volume add new ceph-handlers role from ceph-ansible (`issue#36251 <http://tracker.ceph.com/issues/36251>`_, `pr#24336 <https://github.com/ceph/ceph/pull/24336>`_, Alfredo Deza)
* ceph-volume: adds a --prepare flag to `lvm batch` (`issue#36363 <http://tracker.ceph.com/issues/36363>`_, `pr#24587 <https://github.com/ceph/ceph/pull/24587>`_, Andrew Schoen)
* ceph-volume: add space between words (`pr#26246 <https://github.com/ceph/ceph/pull/26246>`_, Sébastien Han)
* ceph-volume: adds test for `ceph-volume lvm list /dev/sda` (`issue#24784 <http://tracker.ceph.com/issues/24784>`_, `issue#24957 <http://tracker.ceph.com/issues/24957>`_, `pr#23348 <https://github.com/ceph/ceph/pull/23348>`_, Andrew Schoen)
* ceph-volume: Add unit test (`pr#25321 <https://github.com/ceph/ceph/pull/25321>`_, Volker Theile)
* ceph-volume: allow to specify --cluster-fsid instead of reading from ceph.conf (`issue#26953 <http://tracker.ceph.com/issues/26953>`_, `pr#24407 <https://github.com/ceph/ceph/pull/24407>`_, Alfredo Deza)
* ceph-volume: an OSD ID must be exist and be destroyed before reuse (`pr#23093 <https://github.com/ceph/ceph/pull/23093>`_, Andrew Schoen, Ron Allred)
* ceph-volume  batch: allow journal+block.db sizing on the CLI (`issue#36088 <http://tracker.ceph.com/issues/36088>`_, `pr#24201 <https://github.com/ceph/ceph/pull/24201>`_, Alfredo Deza)
* ceph-volume batch: allow --osds-per-device, default it to 1 (`issue#35913 <http://tracker.ceph.com/issues/35913>`_, `pr#24060 <https://github.com/ceph/ceph/pull/24060>`_, Alfredo Deza)
* ceph-volume  batch carve out lvs for bluestore (`pr#24019 <https://github.com/ceph/ceph/pull/24019>`_, Alfredo Deza)
* ceph-volume batch command (`issue#24492 <http://tracker.ceph.com/issues/24492>`_, `pr#23075 <https://github.com/ceph/ceph/pull/23075>`_, Alfredo Deza)
* ceph-volume:  batch tests for mixed-type of devices (`issue#35535 <http://tracker.ceph.com/issues/35535>`_, `issue#27210 <http://tracker.ceph.com/issues/27210>`_, `pr#23963 <https://github.com/ceph/ceph/pull/23963>`_, Alfredo Deza)
* ceph-volume custom cluster names fail on filestore trigger (`issue#27210 <http://tracker.ceph.com/issues/27210>`_, `pr#24251 <https://github.com/ceph/ceph/pull/24251>`_, Alfredo Deza)
* ceph-volume: do not pin the testinfra version for the simple tests (`pr#23268 <https://github.com/ceph/ceph/pull/23268>`_, Andrew Schoen)
* ceph-volume: do not send (lvm) stderr/stdout to the terminal, use the logfile (`issue#36492 <http://tracker.ceph.com/issues/36492>`_, `pr#24738 <https://github.com/ceph/ceph/pull/24738>`_, Alfredo Deza)
* ceph-volume do not use stdin in luminous (`issue#25173 <http://tracker.ceph.com/issues/25173>`_, `pr#23355 <https://github.com/ceph/ceph/pull/23355>`_, Alfredo Deza)
* ceph-volume: don't create osd['block.db'] by default (`issue#38472 <http://tracker.ceph.com/issues/38472>`_, `pr#26627 <https://github.com/ceph/ceph/pull/26627>`_, Jan Fajerski)
* ceph-volume: earlier detection for --journal and --filestore flag requirements (`issue#24794 <http://tracker.ceph.com/issues/24794>`_, `pr#24150 <https://github.com/ceph/ceph/pull/24150>`_, Alfredo Deza)
* ceph-volume: enable device discards (`issue#36532 <http://tracker.ceph.com/issues/36532>`_, `pr#24676 <https://github.com/ceph/ceph/pull/24676>`_, Jonas Jelten)
* ceph-volume enable  --no-systemd flag for simple sub-command (`issue#36470 <http://tracker.ceph.com/issues/36470>`_, `pr#24998 <https://github.com/ceph/ceph/pull/24998>`_, Alfredo Deza)
* ceph-volume: enable the ceph-osd during lvm activation (`issue#24152 <http://tracker.ceph.com/issues/24152>`_, `pr#23321 <https://github.com/ceph/ceph/pull/23321>`_, Dan van der Ster)
* ceph-volume ensure encoded bytes are always used (`issue#24993 <http://tracker.ceph.com/issues/24993>`_, `pr#23289 <https://github.com/ceph/ceph/pull/23289>`_, Alfredo Deza)
* ceph-volume: error on commands that need ceph.conf to operate (`issue#23941 <http://tracker.ceph.com/issues/23941>`_, `pr#22724 <https://github.com/ceph/ceph/pull/22724>`_, Andrew Schoen)
* ceph-volume  expand auto engine for multiple devices on filestore (`issue#24553 <http://tracker.ceph.com/issues/24553>`_, `pr#23731 <https://github.com/ceph/ceph/pull/23731>`_, Andrew Schoen, Alfredo Deza)
* ceph-volume:  expand auto engine for single type devices on filestore (`issue#24960 <http://tracker.ceph.com/issues/24960>`_, `pr#23532 <https://github.com/ceph/ceph/pull/23532>`_, Alfredo Deza)
* ceph-volume expand on the LVM API to create multiple LVs at different sizes (`issue#24020 <http://tracker.ceph.com/issues/24020>`_, `pr#22426 <https://github.com/ceph/ceph/pull/22426>`_, Alfredo Deza)
* ceph-volume: extract flake8 config (`pr#24674 <https://github.com/ceph/ceph/pull/24674>`_, Mehdi Abaakouk)
* ceph-volume: fix Batch object in py3 environments (`pr#25203 <https://github.com/ceph/ceph/pull/25203>`_, Jan Fajerski)
* ceph-volume: fix journal and filestore data size in `lvm batch --report` (`issue#36242 <http://tracker.ceph.com/issues/36242>`_, `pr#24274 <https://github.com/ceph/ceph/pull/24274>`_, Andrew Schoen)
* ceph-volume: fix JSON output in `inventory` (`issue#37390 <http://tracker.ceph.com/issues/37390>`_, `pr#25224 <https://github.com/ceph/ceph/pull/25224>`_, Sebastian Wagner)
* ceph-volume: Fix TypeError: join() takes exactly one argument (2 given) (`issue#37595 <http://tracker.ceph.com/issues/37595>`_, `pr#25469 <https://github.com/ceph/ceph/pull/25469>`_, Sebastian Wagner)
* ceph-volume fix TypeError on dmcrypt when using Python3 (`pr#26034 <https://github.com/ceph/ceph/pull/26034>`_, Alfredo Deza)
* ceph-volume  fix zap not working with LVs (`issue#35970 <http://tracker.ceph.com/issues/35970>`_, `pr#24077 <https://github.com/ceph/ceph/pull/24077>`_, Alfredo Deza)
* ceph-volume: implement __format__ in Size to format sizes in py3 (`issue#38291 <http://tracker.ceph.com/issues/38291>`_, `pr#26401 <https://github.com/ceph/ceph/pull/26401>`_, Jan Fajerski)
* ceph-volume initial take on auto sub-command (`pr#21803 <https://github.com/ceph/ceph/pull/21803>`_, Alfredo Deza)
* ceph-volume: introduce class hierachy for strategies (`issue#37389 <http://tracker.ceph.com/issues/37389>`_, `pr#25238 <https://github.com/ceph/ceph/pull/25238>`_, Jan Fajerski)
* ceph-volume:  lsblk can fail to find PARTLABEL, must fallback to blkid (`issue#36098 <http://tracker.ceph.com/issues/36098>`_, `pr#24330 <https://github.com/ceph/ceph/pull/24330>`_, Alfredo Deza)
* ceph-volume lvm.activate conditional mon-config on prime-osd-dir (`issue#25216 <http://tracker.ceph.com/issues/25216>`_, `pr#23375 <https://github.com/ceph/ceph/pull/23375>`_, Alfredo Deza)
* ceph-volume lvm.activate Do not search for a MON configuration (`pr#22393 <https://github.com/ceph/ceph/pull/22393>`_, Wido den Hollander)
* ceph-volume: `lvm batch` allow extra flags (like dmcrypt) for bluestore (`issue#26862 <http://tracker.ceph.com/issues/26862>`_, `pr#23448 <https://github.com/ceph/ceph/pull/23448>`_, Alfredo Deza)
* ceph-volume lvm.batch remove non-existent sys_api property (`issue#34310 <http://tracker.ceph.com/issues/34310>`_, `pr#23787 <https://github.com/ceph/ceph/pull/23787>`_, Alfredo Deza)
* ceph-volume lvm.listing only include devices if they exist (`issue#24952 <http://tracker.ceph.com/issues/24952>`_, `pr#23129 <https://github.com/ceph/ceph/pull/23129>`_, Alfredo Deza)
* ceph-volume lvm.prepare update help to indicate partitions are needed, not devices (`issue#24795 <http://tracker.ceph.com/issues/24795>`_, `pr#24394 <https://github.com/ceph/ceph/pull/24394>`_, Alfredo Deza)
* ceph-volume: make Device hashable to allow set of Device list in py3 (`issue#38290 <http://tracker.ceph.com/issues/38290>`_, `pr#26399 <https://github.com/ceph/ceph/pull/26399>`_, Jan Fajerski)
* ceph-volume: make `lvm batch` idempotent (`issue#26864 <http://tracker.ceph.com/issues/26864>`_, `pr#24404 <https://github.com/ceph/ceph/pull/24404>`_, Andrew Schoen)
* ceph-volume: mark a device not available if it belongs to ceph-disk (`pr#26084 <https://github.com/ceph/ceph/pull/26084>`_, Andrew Schoen)
* ceph-volume normalize comma to dot for string to int conversions (`issue#37442 <http://tracker.ceph.com/issues/37442>`_, `pr#25674 <https://github.com/ceph/ceph/pull/25674>`_, Alfredo Deza)
* ceph-volume: patch Device when testing (`issue#36768 <http://tracker.ceph.com/issues/36768>`_, `pr#25063 <https://github.com/ceph/ceph/pull/25063>`_, Alfredo Deza)
* ceph-volume process.call with stdin in Python 3 fix (`issue#24993 <http://tracker.ceph.com/issues/24993>`_, `pr#23141 <https://github.com/ceph/ceph/pull/23141>`_, Alfredo Deza)
* ceph-volume: provide a nice errror message when missing ceph.conf (`pr#22828 <https://github.com/ceph/ceph/pull/22828>`_, Andrew Schoen)
* ceph-volume: PVolumes.get() should return one PV when using name or uuid (`issue#24784 <http://tracker.ceph.com/issues/24784>`_, `pr#23234 <https://github.com/ceph/ceph/pull/23234>`_, Andrew Schoen)
* ceph-volume: refuse to zap mapper devices (`issue#24504 <http://tracker.ceph.com/issues/24504>`_, `pr#22764 <https://github.com/ceph/ceph/pull/22764>`_, Andrew Schoen)
* ceph-volume: reject devices that have existing GPT headers (`issue#27062 <http://tracker.ceph.com/issues/27062>`_, `pr#25098 <https://github.com/ceph/ceph/pull/25098>`_, Andrew Schoen)
* ceph-volume: remove iteritems instances (`issue#38299 <http://tracker.ceph.com/issues/38299>`_, `pr#26403 <https://github.com/ceph/ceph/pull/26403>`_, Jan Fajerski)
* ceph-volume: remove LVs when using zap --destroy (`pr#25093 <https://github.com/ceph/ceph/pull/25093>`_, Alfredo Deza)
* ceph-volume remove version reporting from help menu (`issue#36386 <http://tracker.ceph.com/issues/36386>`_, `pr#24531 <https://github.com/ceph/ceph/pull/24531>`_, Alfredo Deza)
* ceph-volume: rename Device property valid to available (`issue#36701 <http://tracker.ceph.com/issues/36701>`_, `pr#25007 <https://github.com/ceph/ceph/pull/25007>`_, Jan Fajerski)
* ceph-volume: replace testinfra command with py.test (`issue#38568 <http://tracker.ceph.com/issues/38568>`_, `pr#26739 <https://github.com/ceph/ceph/pull/26739>`_, Alfredo Deza)
* ceph-volume: Restore SELinux context (`pr#23278 <https://github.com/ceph/ceph/pull/23278>`_, Boris Ranto)
* ceph-volume: revert partition as disk (`issue#37506 <http://tracker.ceph.com/issues/37506>`_, `pr#25390 <https://github.com/ceph/ceph/pull/25390>`_, Jan Fajerski)
* ceph-volume: run tests without waiting on ceph repos (`pr#23697 <https://github.com/ceph/ceph/pull/23697>`_, Andrew Schoen)
* ceph-volume: set number of osd ports in the tests (`pr#26753 <https://github.com/ceph/ceph/pull/26753>`_, Andrew Schoen)
* ceph-volume: set permissions right before prime-osd-dir (`issue#37486 <http://tracker.ceph.com/issues/37486>`_, `pr#25477 <https://github.com/ceph/ceph/pull/25477>`_, Andrew Schoen, Alfredo Deza)
* ceph-volume: `simple scan` will now scan all running ceph-disk OSDs (`pr#26826 <https://github.com/ceph/ceph/pull/26826>`_, Andrew Schoen)
* ceph-volume: skip processing devices that don't exist when scanning system disks (`issue#36247 <http://tracker.ceph.com/issues/36247>`_, `pr#24372 <https://github.com/ceph/ceph/pull/24372>`_, Alfredo Deza)
* ceph-volume: sort and align `lvm list` output (`pr#21812 <https://github.com/ceph/ceph/pull/21812>`_, Theofilos Mouratidis)
* ceph-volume systemd import main so console_scripts work for executable (`issue#36648 <http://tracker.ceph.com/issues/36648>`_, `pr#24840 <https://github.com/ceph/ceph/pull/24840>`_, Alfredo Deza)
* ceph-volume tests destroy osds on monitor hosts (`pr#22437 <https://github.com/ceph/ceph/pull/22437>`_, Alfredo Deza)
* ceph-volume tests do not include admin keyring in OSD nodes (`issue#24417 <http://tracker.ceph.com/issues/24417>`_, `pr#22399 <https://github.com/ceph/ceph/pull/22399>`_, Alfredo Deza)
* ceph-volume tests/functional add mgrs daemons to lvm tests (`issue#26879 <http://tracker.ceph.com/issues/26879>`_, `pr#23489 <https://github.com/ceph/ceph/pull/23489>`_, Alfredo Deza)
* ceph-volume tests.functional add notario dep for ceph-ansible (`pr#22116 <https://github.com/ceph/ceph/pull/22116>`_, Alfredo Deza)
* ceph-volume tests/functional declare ceph-ansible roles instead of importing them (`issue#37805 <http://tracker.ceph.com/issues/37805>`_, `pr#25820 <https://github.com/ceph/ceph/pull/25820>`_, Alfredo Deza)
* ceph-volume tests.functional fix typo when stopping osd.0 in filestore (`issue#37675 <http://tracker.ceph.com/issues/37675>`_, `pr#25594 <https://github.com/ceph/ceph/pull/25594>`_, Alfredo Deza)
* ceph-volume: tests.functional inherit SSH_ARGS from ansible (`issue#34311 <http://tracker.ceph.com/issues/34311>`_, `pr#23788 <https://github.com/ceph/ceph/pull/23788>`_, Alfredo Deza)
* ceph-volume tests/functional run lvm list after OSD provisioning (`issue#24961 <http://tracker.ceph.com/issues/24961>`_, `pr#23116 <https://github.com/ceph/ceph/pull/23116>`_, Alfredo Deza)
* ceph-volume tests/functional use Ansible 2.6 (`pr#23182 <https://github.com/ceph/ceph/pull/23182>`_, Alfredo Deza)
* ceph-volume tests install ceph-ansible's requirements.txt dependencies (`issue#36672 <http://tracker.ceph.com/issues/36672>`_, `pr#24881 <https://github.com/ceph/ceph/pull/24881>`_, Alfredo Deza)
* ceph-volume tests patch __release__ to mimic always for stdin keys (`pr#23398 <https://github.com/ceph/ceph/pull/23398>`_, Alfredo Deza)
* ceph-volume tests.systemd update imports for systemd module (`issue#36704 <http://tracker.ceph.com/issues/36704>`_, `pr#24937 <https://github.com/ceph/ceph/pull/24937>`_, Alfredo Deza)
* ceph-volume: test with multiple NVME drives (`issue#37409 <http://tracker.ceph.com/issues/37409>`_, `pr#25354 <https://github.com/ceph/ceph/pull/25354>`_, Andrew Schoen)
* ceph-volume: unmount lvs correctly before zapping (`issue#24796 <http://tracker.ceph.com/issues/24796>`_, `pr#23117 <https://github.com/ceph/ceph/pull/23117>`_, Andrew Schoen)
* ceph-volume: update testing playbook 'deploy.yml' (`pr#26397 <https://github.com/ceph/ceph/pull/26397>`_, Guillaume Abrioux)
* ceph-volume: update version of ansible to 2.6.x for simple tests (`pr#23263 <https://github.com/ceph/ceph/pull/23263>`_, Andrew Schoen)
* ceph-volume: use console_scripts (`issue#36601 <http://tracker.ceph.com/issues/36601>`_, `pr#24773 <https://github.com/ceph/ceph/pull/24773>`_, Mehdi Abaakouk)
* ceph-volume: use our own testinfra suite for functional testing (`pr#26685 <https://github.com/ceph/ceph/pull/26685>`_, Andrew Schoen)
* ceph-volume util.encryption don't push stderr to terminal (`issue#36246 <http://tracker.ceph.com/issues/36246>`_, `pr#24399 <https://github.com/ceph/ceph/pull/24399>`_, Alfredo Deza)
* ceph-volume util.encryption robust blkid+lsblk detection of lockbox (`pr#24977 <https://github.com/ceph/ceph/pull/24977>`_, Alfredo Deza)
* ceph-volume zap devices associated with an OSD ID and/or OSD FSID (`pr#25429 <https://github.com/ceph/ceph/pull/25429>`_, Alfredo Deza)
* ceph-volume  zap: improve zapping to remove all partitions and all LVs, encrypted or not (`issue#37449 <http://tracker.ceph.com/issues/37449>`_, `pr#25330 <https://github.com/ceph/ceph/pull/25330>`_, Alfredo Deza)
* cleanup: Clean up warnings (`pr#23919 <https://github.com/ceph/ceph/pull/23919>`_, Adam C. Emerson)
* cli: dump osd-fsid as part of osd find <id> (`pr#26015 <https://github.com/ceph/ceph/pull/26015>`_, Noah Watkins)
* cmake: add "add_npm_command()" command (`pr#22636 <https://github.com/ceph/ceph/pull/22636>`_, Kefu Chai)
* cmake: Add cls_opt for vstart target (`pr#22538 <https://github.com/ceph/ceph/pull/22538>`_, Ali Maredia)
* cmake: add dpdk::dpdk if dpdk is built or found (`issue#24948 <http://tracker.ceph.com/issues/24948>`_, `pr#23620 <https://github.com/ceph/ceph/pull/23620>`_, Nathan Cutler, Kefu Chai)
* cmake: add option WITH_LIBRADOSSTRIPER (`pr#23732 <https://github.com/ceph/ceph/pull/23732>`_, Kefu Chai)
* cmake: allow setting of the CTest timeout during building (`pr#22800 <https://github.com/ceph/ceph/pull/22800>`_, Willem Jan Withagen)
* cmake: always prefer local symbols (`issue#25154 <http://tracker.ceph.com/issues/25154>`_, `pr#23320 <https://github.com/ceph/ceph/pull/23320>`_, Kefu Chai)
* cmake: always turn off bjam debugging output (`pr#22204 <https://github.com/ceph/ceph/pull/22204>`_, Kefu Chai)
* cmake: bump up the required boost version to 1.67 (`pr#22392 <https://github.com/ceph/ceph/pull/22392>`_, Kefu Chai)
* cmake: bump up the required fmt version (`pr#23283 <https://github.com/ceph/ceph/pull/23283>`_, Kefu Chai)
* cmake: cleanups (`pr#23166 <https://github.com/ceph/ceph/pull/23166>`_, Kefu Chai)
* cmake: cleanups (`pr#23279 <https://github.com/ceph/ceph/pull/23279>`_, Kefu Chai)
* cmake: cleanups (`pr#23300 <https://github.com/ceph/ceph/pull/23300>`_, Kefu Chai)
* cmake,crimson/net: add keepalive support, and enable unittest_seastar_messenger in "make check" (`pr#23642 <https://github.com/ceph/ceph/pull/23642>`_, Kefu Chai)
* cmake: detect armv8 crc and crypto feature using CHECK_C_COMPILER_FLAG (`issue#17516 <http://tracker.ceph.com/issues/17516>`_, `pr#24168 <https://github.com/ceph/ceph/pull/24168>`_, Kefu Chai)
* cmake: disable -Werror-stringop-truncation for rocksdb (`pr#22591 <https://github.com/ceph/ceph/pull/22591>`_, Kefu Chai)
* cmake: do not check for aligned_alloc() anymore (`issue#23653 <http://tracker.ceph.com/issues/23653>`_, `pr#22046 <https://github.com/ceph/ceph/pull/22046>`_, Kefu Chai)
* cmake: do not depend on ${DPDK_LIBRARIES} if not using bundled dpdk (`issue#24449 <http://tracker.ceph.com/issues/24449>`_, `pr#22938 <https://github.com/ceph/ceph/pull/22938>`_, Kefu Chai)
* cmake: do not install `hello` demo module (`pr#21886 <https://github.com/ceph/ceph/pull/21886>`_, John Spray)
* cmake: do not link against common_crc_aarch64 (`pr#23366 <https://github.com/ceph/ceph/pull/23366>`_, Kefu Chai)
* cmake: do not pass -B{symbolic,symbolic-functions} to linker on FreeBSD (`pr#24920 <https://github.com/ceph/ceph/pull/24920>`_, Willem Jan Withagen)
* cmake: do not pass unnecessary param to setup.py (`pr#25186 <https://github.com/ceph/ceph/pull/25186>`_, Kefu Chai)
* cmake: do not use Findfmt.cmake for checking libfmt-dev (`pr#23390 <https://github.com/ceph/ceph/pull/23390>`_, Kefu Chai)
* cmake: do not use plain target_link_libraries(rgw_a ...) (`pr#24515 <https://github.com/ceph/ceph/pull/24515>`_, Kefu Chai)
* cmake: enable RTTI for both debug and release RocksDB builds (`pr#22286 <https://github.com/ceph/ceph/pull/22286>`_, Igor Fedotov)
* cmake: find a python2 interpreter for gtest-parallel (`pr#22931 <https://github.com/ceph/ceph/pull/22931>`_, Kefu Chai)
* cmake: find liboath using the correct name (`pr#22430 <https://github.com/ceph/ceph/pull/22430>`_, Kefu Chai)
* cmake: fix a cmake error when with -DALLOCATOR=jemalloc (`pr#23380 <https://github.com/ceph/ceph/pull/23380>`_, Jianpeng Ma)
* cmake: fix build WITH_SYSTEM_BOOST=ON (`pr#23510 <https://github.com/ceph/ceph/pull/23510>`_, Kefu Chai)
* cmake: fix compilation with distcc and other compiler wrappers (`pr#24605 <https://github.com/ceph/ceph/pull/24605>`_, Alexey Sheplyakov, Kefu Chai)
* cmake: fix cython target in test/CMakeFile.txt (`pr#22295 <https://github.com/ceph/ceph/pull/22295>`_, Jan Fajerski)
* cmake: fix Debug build `WITH_SEASTAR=ON` (`pr#23567 <https://github.com/ceph/ceph/pull/23567>`_, Kefu Chai)
* cmake: fixes to enable WITH_ASAN with clang and GCC (`pr#24692 <https://github.com/ceph/ceph/pull/24692>`_, Kefu Chai)
* cmake: fix find system rockdb (`pr#22439 <https://github.com/ceph/ceph/pull/22439>`_, Alexey Shabalin)
* cmake: fix std::filesystem detection and extract sanitizer detection into its own module (`pr#23384 <https://github.com/ceph/ceph/pull/23384>`_, Kefu Chai)
* cmake: fix syntax error of set() (`pr#26582 <https://github.com/ceph/ceph/pull/26582>`_, Kefu Chai)
* cmake: fix the build WITH_DPDK=ON (`pr#23650 <https://github.com/ceph/ceph/pull/23650>`_, Kefu Chai, Casey Bodley)
* cmake: fix version matching for Findfmt (`pr#23996 <https://github.com/ceph/ceph/pull/23996>`_, Mohamad Gebai)
* cmake: fix "WITH_STATIC_LIBSTDCXX" (`pr#22990 <https://github.com/ceph/ceph/pull/22990>`_, Kefu Chai)
* cmake: let rbd_api depend on librbd-tp (`pr#25641 <https://github.com/ceph/ceph/pull/25641>`_, Kefu Chai)
* cmake: link against gtest in a better way (`pr#23628 <https://github.com/ceph/ceph/pull/23628>`_, Kefu Chai)
* cmake: link ceph-osd with common statically (`pr#22720 <https://github.com/ceph/ceph/pull/22720>`_, Radoslaw Zarzynski)
* cmake: link compressor plugins against lib the modern way (`pr#23852 <https://github.com/ceph/ceph/pull/23852>`_, Kefu Chai)
* cmake: make -DWITH_MGR=OFF work (`pr#22077 <https://github.com/ceph/ceph/pull/22077>`_, Jianpeng Ma)
* cmake: Make the tests for finding Filesystem with more serious functions (`pr#26316 <https://github.com/ceph/ceph/pull/26316>`_, Willem Jan Withagen)
* cmake: modularize src/perfglue (`pr#23254 <https://github.com/ceph/ceph/pull/23254>`_, Kefu Chai)
* cmake: move ceph-osdomap-tool, ceph-monstore-tool out of ceph-test (`pr#19964 <https://github.com/ceph/ceph/pull/19964>`_, runsisi)
* cmake: move crypto_plugins target (`pr#21891 <https://github.com/ceph/ceph/pull/21891>`_, Casey Bodley)
* cmake: no libradosstriper headers if WITH_LIBRADOSSTRIPER=OFF (`issue#35922 <http://tracker.ceph.com/issues/35922>`_, `pr#24029 <https://github.com/ceph/ceph/pull/24029>`_, Nathan Cutler, Kefu Chai)
* cmake: no need to add "-D" before definitions (`pr#23795 <https://github.com/ceph/ceph/pull/23795>`_, Kefu Chai)
* cmake: oath lives in liboath (`pr#22494 <https://github.com/ceph/ceph/pull/22494>`_, Willem Jan Withagen)
* cmake: only build extra boost libraries only if WITH_SEASTAR (`pr#22521 <https://github.com/ceph/ceph/pull/22521>`_, Kefu Chai)
* cmake: remove checking for GCC 5.1 (`pr#24477 <https://github.com/ceph/ceph/pull/24477>`_, Kefu Chai)
* cmake: remove deleted rgw_request.cc from CMakeLists.txt (`pr#22186 <https://github.com/ceph/ceph/pull/22186>`_, Casey Bodley)
* cmake: Remove embedded 'cephd' code (`pr#21940 <https://github.com/ceph/ceph/pull/21940>`_, Dan Mick)
* cmake: remove workarounds for supporting cmake 2.x (`pr#22912 <https://github.com/ceph/ceph/pull/22912>`_, Kefu Chai)
* cmake: rgw_common should depend on tracing headers (`pr#22367 <https://github.com/ceph/ceph/pull/22367>`_, Kefu Chai)
* cmake: rocksdb related cleanup (`pr#23441 <https://github.com/ceph/ceph/pull/23441>`_, Kefu Chai)
* cmake: should link against libatomic if libcxx/libstdc++ does not off… (`pr#22952 <https://github.com/ceph/ceph/pull/22952>`_, Kefu Chai)
* cmake: update fio version from 3.5 to 540e235dcd276e63c57 (`pr#22019 <https://github.com/ceph/ceph/pull/22019>`_, Jianpeng Ma)
* cmake: use $CMAKE_BINARY_DIR for default $CEPH_BUILD_VIRTUALENV (`issue#36737 <http://tracker.ceph.com/issues/36737>`_, `pr#26091 <https://github.com/ceph/ceph/pull/26091>`_, Kefu Chai)
* cmake: use javac -h for creating JNI native headers (`issue#24012 <http://tracker.ceph.com/issues/24012>`_, `pr#21822 <https://github.com/ceph/ceph/pull/21822>`_, Kefu Chai)
* cmake: use OpenSSL::Crypto instead of OPENSSL_LIBRARIES (`pr#24368 <https://github.com/ceph/ceph/pull/24368>`_, Kefu Chai)
* cmake: vstart target can build WITH_CEPHFS/RBD/MGR=OFF (`pr#25204 <https://github.com/ceph/ceph/pull/25204>`_, Casey Bodley)
* common: add adaptor for seastar::temporary_buffer (`pr#22454 <https://github.com/ceph/ceph/pull/22454>`_, Kefu Chai, Casey Bodley)
* common: add a generic async Completion for use with boost::asio (`pr#21914 <https://github.com/ceph/ceph/pull/21914>`_, Casey Bodley)
* common: add lockless `md_config_t` (`pr#22710 <https://github.com/ceph/ceph/pull/22710>`_, Kefu Chai)
* common: async/dpdk: when enable dpdk, multiple message queue defect (`pr#25404 <https://github.com/ceph/ceph/pull/25404>`_, zhangyongsheng)
* common: auth/cephx: minor code cleanup (`pr#21155 <https://github.com/ceph/ceph/pull/21155>`_, runsisi)
* common: auth, common: cleanups (`pr#26383 <https://github.com/ceph/ceph/pull/26383>`_, Kefu Chai)
* common: auth,common: use ceph::mutex instead of LockMutex (`pr#24263 <https://github.com/ceph/ceph/pull/24263>`_, Kefu Chai)
* common: avoid the overhead of ``ANNOTATE_HAPPENS_*`` in NDEBUG builds (`pr#25129 <https://github.com/ceph/ceph/pull/25129>`_, Radoslaw Zarzynski)
* common: be more informative if set PID-file fails (`pr#23647 <https://github.com/ceph/ceph/pull/23647>`_, Willem Jan Withagen)
* common: blkdev: Rework API and add FreeBSD support (`pr#24658 <https://github.com/ceph/ceph/pull/24658>`_, Alan Somers)
* common: buffer: mark the iterator traits "public" (`pr#25409 <https://github.com/ceph/ceph/pull/25409>`_, Kefu Chai)
* common: calculate stddev on the fly (`pr#21461 <https://github.com/ceph/ceph/pull/21461>`_, Yao Zongyou)
* common: ceph.in: use correct module for cmd flags (`pr#26454 <https://github.com/ceph/ceph/pull/26454>`_, Patrick Donnelly)
* common: ceph-volume add device_id to inventory listing (`pr#25201 <https://github.com/ceph/ceph/pull/25201>`_, Jan Fajerski)
* common: changes to address FTBFS on fc30 (`pr#26301 <https://github.com/ceph/ceph/pull/26301>`_, Kefu Chai)
* common: common/admin_socket: add new api unregister_commands(AdminSocketHook … (`pr#21718 <https://github.com/ceph/ceph/pull/21718>`_, Jianpeng Ma)
* common: common,auth,crimson: add logging to crimson (`pr#23957 <https://github.com/ceph/ceph/pull/23957>`_, Kefu Chai)
* common: common/buffer: fix compiler bug when enable DEBUG_BUFFER (`pr#25848 <https://github.com/ceph/ceph/pull/25848>`_, Jianpeng Ma)
* common: common/buffer: remove repeated condtion-check (`pr#25420 <https://github.com/ceph/ceph/pull/25420>`_, Jianpeng Ma)
* common: common/config: add ConfigProxy for crimson (`pr#23074 <https://github.com/ceph/ceph/pull/23074>`_, Kefu Chai)
* common: common/config: fix the lock in ConfigProxy::diff() (`pr#23276 <https://github.com/ceph/ceph/pull/23276>`_, Kefu Chai)
* common: common/config_values: friend md_config_impl<> (`pr#23020 <https://github.com/ceph/ceph/pull/23020>`_, Mykola Golub, Kefu Chai)
* common:  common: drop the unused methods from SharedLRU (`pr#26224 <https://github.com/ceph/ceph/pull/26224>`_, Radoslaw Zarzynski)
* common: common/KeyValueDB: Get rid of validate parameter (`pr#25377 <https://github.com/ceph/ceph/pull/25377>`_, Adam Kupczyk)
* common: common/numa: Add shim routines for NUMA on FreeBSD (`pr#25920 <https://github.com/ceph/ceph/pull/25920>`_, Willem Jan Withagen)
* common: common, osd: set mclock priority as 1 by default (`pr#26022 <https://github.com/ceph/ceph/pull/26022>`_, Abhishek Lekshmanan)
* common: common/random_cache: remove unused RandomCache (`pr#26253 <https://github.com/ceph/ceph/pull/26253>`_, Kefu Chai)
* common: common/shared_cache: add lockless SharedLRU (`pr#22736 <https://github.com/ceph/ceph/pull/22736>`_, Kefu Chai)
* common: common/shared_cache: bumps it to the front of the LRU if key existed (`pr#25370 <https://github.com/ceph/ceph/pull/25370>`_, Jianpeng Ma)
* common: common/shared_cache: fix racing issues (`pr#25150 <https://github.com/ceph/ceph/pull/25150>`_, Jianpeng Ma)
* common: common/util: pass real hostname when running in kubernetes/rook container (`pr#23798 <https://github.com/ceph/ceph/pull/23798>`_, Sage Weil)
* common: complete all throttle blockers when we set average or max to 0 (`issue#36715 <http://tracker.ceph.com/issues/36715>`_, `pr#24965 <https://github.com/ceph/ceph/pull/24965>`_, Dongsheng Yang)
* common,core: msg/async: clean up local buffers on dispatch (`issue#35987 <http://tracker.ceph.com/issues/35987>`_, `pr#24111 <https://github.com/ceph/ceph/pull/24111>`_, Greg Farnum)
* common,core,tests: qa/tests: update links for centos latest to point to 7.5 (`pr#22923 <https://github.com/ceph/ceph/pull/22923>`_, Vasu Kulkarni)
* common/crc/aarch64: Added cpu feature pmull and make aarch64 specific… (`pr#22178 <https://github.com/ceph/ceph/pull/22178>`_, Adam Kupczyk)
* common:  crimson/common: write configs synchronously on shard.0 (`pr#23284 <https://github.com/ceph/ceph/pull/23284>`_, Kefu Chai)
* common,crimson: port perfcounters to seastar (`pr#24141 <https://github.com/ceph/ceph/pull/24141>`_, chunmei Liu)
* common: crypto: QAT based Encryption for RGW (`pr#19386 <https://github.com/ceph/ceph/pull/19386>`_, Ganesh Maharaj Mahalingam)
* common: crypto: use ceph_assert_always for assertions (`pr#23654 <https://github.com/ceph/ceph/pull/23654>`_, Casey Bodley)
* common: define BOOST_COROUTINES_NO_DEPRECATION_WARNING if not yet (`pr#26502 <https://github.com/ceph/ceph/pull/26502>`_, Kefu Chai)
* common: drop allocation tracking from bufferlist (`pr#25454 <https://github.com/ceph/ceph/pull/25454>`_, Radoslaw Zarzynski)
* common: drop append_buffer from bufferlist. Use simple carriage instead (`pr#25077 <https://github.com/ceph/ceph/pull/25077>`_, Radoslaw Zarzynski)
* common: drop at_buffer_{head,tail} from buffer::ptr (`pr#25422 <https://github.com/ceph/ceph/pull/25422>`_, Radoslaw Zarzynski)
* common: drop/mark-as-final getters of buffer::raw for palign (`pr#24087 <https://github.com/ceph/ceph/pull/24087>`_, Radoslaw Zarzynski)
* common: drop static_assert.h as it looks unused (`pr#22743 <https://github.com/ceph/ceph/pull/22743>`_, Radoslaw Zarzynski)
* common: drop the unused buffer::raw_mmap_pages (`pr#24040 <https://github.com/ceph/ceph/pull/24040>`_, Radoslaw Zarzynski)
* common: drop the unused zero-copy facilities in ceph::bufferlist (`pr#24031 <https://github.com/ceph/ceph/pull/24031>`_, Radoslaw Zarzynski)
* common: drop unused get_max_pipe_size() in buffer.cc (`pr#25432 <https://github.com/ceph/ceph/pull/25432>`_, Radoslaw Zarzynski)
* common: ec: lrc doesn't depend on crosstalks between bufferlists anymore (`pr#25595 <https://github.com/ceph/ceph/pull/25595>`_, Radoslaw Zarzynski)
* common: expand meta in parse_argv() (`pr#23474 <https://github.com/ceph/ceph/pull/23474>`_, Kefu Chai)
* common: fix access and add name for the token bucket throttle (`pr#25372 <https://github.com/ceph/ceph/pull/25372>`_, Shiyang Ruan)
* common: Fix Alpine compatability for TEMP_FAILURE_RETRY and ACCESSPERMS (`pr#24813 <https://github.com/ceph/ceph/pull/24813>`_, Willem Jan Withagen)
* common: fix a racing in PerfCounters::perf_counter_data_any_d::read_avg (`issue#25211 <http://tracker.ceph.com/issues/25211>`_, `pr#23362 <https://github.com/ceph/ceph/pull/23362>`_, ludehp)
* common: fix for broken rbdmap parameter parsing (`pr#24446 <https://github.com/ceph/ceph/pull/24446>`_, Marc Schoechlin)
* common: fix missing include boost/noncopyable.hpp (`pr#24278 <https://github.com/ceph/ceph/pull/24278>`_, Willem Jan Withagen)
* common: fix typo in rados bench write JSON output (`issue#24199 <http://tracker.ceph.com/issues/24199>`_, `pr#22112 <https://github.com/ceph/ceph/pull/22112>`_, Sandor Zeestraten)
* common: fix typos in BackoffThrottle (`pr#24691 <https://github.com/ceph/ceph/pull/24691>`_, Shiyang Ruan)
* common: Formatters: improve precision of double numbers (`pr#25745 <https://github.com/ceph/ceph/pull/25745>`_, Коренберг Марк)
* common: .gitignore: Ignore .idea directory (`pr#24237 <https://github.com/ceph/ceph/pull/24237>`_, Volker Theile)
* common: hint bufferlist's buffer_track_c_str accordingly (`pr#25424 <https://github.com/ceph/ceph/pull/25424>`_, Radoslaw Zarzynski)
* common: hypercombined bufferlist (`pr#24882 <https://github.com/ceph/ceph/pull/24882>`_, Radoslaw Zarzynski)
* common: include/compat.h: make pthread_get_name_np work when available (`pr#23641 <https://github.com/ceph/ceph/pull/23641>`_, Willem Jan Withagen)
* common: include include/types.h early, otherwise Clang will error (`pr#22493 <https://github.com/ceph/ceph/pull/22493>`_, Willem Jan Withagen)
* common: include/types: move operator<< into the proper namespace (`pr#23767 <https://github.com/ceph/ceph/pull/23767>`_, Kefu Chai)
* common: include/types: space between number and units (`pr#22063 <https://github.com/ceph/ceph/pull/22063>`_, Sage Weil)
* common: librados,rpm,deb: various fixes to address librados3 transition and cleanups in librados (`pr#24896 <https://github.com/ceph/ceph/pull/24896>`_, Kefu Chai)
* common: make CEPH_BUFFER_ALLOC_UNIT known at compile-time (`pr#26259 <https://github.com/ceph/ceph/pull/26259>`_, Radoslaw Zarzynski)
* common: mark BlkDev::serial() const to match with its declaration (`pr#24702 <https://github.com/ceph/ceph/pull/24702>`_, Willem Jan Withagen)
* common: messages: define HEAD_VERSION and COMPAT_VERSION inlined (`pr#23623 <https://github.com/ceph/ceph/pull/23623>`_, Kefu Chai)
* common,mgr: mgr/MgrClient: make some noise for a user if no mgr daemon is running (`pr#23492 <https://github.com/ceph/ceph/pull/23492>`_, Sage Weil)
* common: mon/MonClient: set configs via finisher (`issue#24118 <http://tracker.ceph.com/issues/24118>`_, `pr#21984 <https://github.com/ceph/ceph/pull/21984>`_, Sage Weil)
* common: msg/async: fix FTBFS of dpdk (`pr#23168 <https://github.com/ceph/ceph/pull/23168>`_, Kefu Chai)
* common: msg/async: Skip the duplicated processing of the same link (`pr#20952 <https://github.com/ceph/ceph/pull/20952>`_, shangfufei)
* common: msg/msg_types.h: do not cast `ceph_entity_name` to `entity_name_t` for printing (`pr#26315 <https://github.com/ceph/ceph/pull/26315>`_, Kefu Chai)
* common: msgr/async/rdma: Return from poll system call with EINTR should be retried (`pr#25138 <https://github.com/ceph/ceph/pull/25138>`_, Stig Telfer)
* common: Mutex -> ceph::mutex (`issue#12614 <http://tracker.ceph.com/issues/12614>`_, `pr#25105 <https://github.com/ceph/ceph/pull/25105>`_, Kefu Chai, Sage Weil)
* common: optimize reference counting in bufferlist (`pr#25082 <https://github.com/ceph/ceph/pull/25082>`_, Radoslaw Zarzynski)
* common: OpTracker doesn't visit TrackedOp when nref == 0 (`issue#24037 <http://tracker.ceph.com/issues/24037>`_, `pr#22156 <https://github.com/ceph/ceph/pull/22156>`_, Radoslaw Zarzynski)
* common: os/filestore: fix throttle configurations (`pr#21926 <https://github.com/ceph/ceph/pull/21926>`_, Li Wang)
* common,performance: auth,common: add lockless auth (`pr#23591 <https://github.com/ceph/ceph/pull/23591>`_, Kefu Chai)
* common,performance: common/assert: mark assert helpers with [[gnu::cold]] (`pr#23326 <https://github.com/ceph/ceph/pull/23326>`_, Kefu Chai)
* common,performance: compressor: add QAT support (`pr#19714 <https://github.com/ceph/ceph/pull/19714>`_, Qiaowei Ren)
* common,performance: denc: fix internal fragmentation when decoding ptr in bl (`pr#25264 <https://github.com/ceph/ceph/pull/25264>`_, Kefu Chai)
* common,rbd: misc: mark constructors as explicit (`pr#21637 <https://github.com/ceph/ceph/pull/21637>`_, Danny Al-Gaaf)
* common: reinit StackStringStream on clear (`pr#25751 <https://github.com/ceph/ceph/pull/25751>`_, Patrick Donnelly)
* common: reintroduce async SharedMutex (`issue#24124 <http://tracker.ceph.com/issues/24124>`_, `pr#22698 <https://github.com/ceph/ceph/pull/22698>`_, Casey Bodley)
* common: Reverse deleted include (`pr#23838 <https://github.com/ceph/ceph/pull/23838>`_, Willem Jan Withagen)
* common: Revert "common: add an async SharedMutex" (`issue#24124 <http://tracker.ceph.com/issues/24124>`_, `pr#21986 <https://github.com/ceph/ceph/pull/21986>`_, Casey Bodley)
* common,rgw: cls/rbd: init local var with known value (`pr#25588 <https://github.com/ceph/ceph/pull/25588>`_, Kefu Chai)
* common,tests: run-standalone.sh: Need double-quotes to handle | in core_pattern on all distributions (`issue#38325 <http://tracker.ceph.com/issues/38325>`_, `pr#26436 <https://github.com/ceph/ceph/pull/26436>`_, David Zafman)
* common,tests: test_shared_cache: fix memory leak (`pr#25215 <https://github.com/ceph/ceph/pull/25215>`_, Jianpeng Ma)
* common: vstart: do not attempt to re-initialize dashboard for existing cluster (`pr#23261 <https://github.com/ceph/ceph/pull/23261>`_, Jason Dillaman)
* core: Add support for osd_delete_sleep configuration value (`issue#36474 <http://tracker.ceph.com/issues/36474>`_, `pr#24749 <https://github.com/ceph/ceph/pull/24749>`_, David Zafman)
* core: auth: drop the RWLock in AuthClientHandler (`pr#23699 <https://github.com/ceph/ceph/pull/23699>`_, Kefu Chai)
* core: auth/krb: Fix Kerberos build warnings (`pr#25639 <https://github.com/ceph/ceph/pull/25639>`_, Daniel Oliveira)
* core: build: disable kerberos for nautilus (`pr#26258 <https://github.com/ceph/ceph/pull/26258>`_, Sage Weil)
* core: ceph_argparse: fix --verbose (`pr#25961 <https://github.com/ceph/ceph/pull/25961>`_, Patrick Nawracay)
* core: ceph.in: friendlier message on EPERM (`issue#25172 <http://tracker.ceph.com/issues/25172>`_, `pr#23330 <https://github.com/ceph/ceph/pull/23330>`_, John Spray)
* core: ceph.in: write bytes to stdout in raw_write() (`pr#25280 <https://github.com/ceph/ceph/pull/25280>`_, Kefu Chai)
* core: ceph_test_rados_api_misc: remove obsolete LibRadosMiscPool.PoolCreationRace (`issue#24150 <http://tracker.ceph.com/issues/24150>`_, `pr#22042 <https://github.com/ceph/ceph/pull/22042>`_, Sage Weil)
* core: Clang misses <optional> include (`pr#23768 <https://github.com/ceph/ceph/pull/23768>`_, Willem Jan Withagen)
* core: common/blkdev.h: use std::string (`pr#25783 <https://github.com/ceph/ceph/pull/25783>`_, Neha Ojha)
* core: common/options: remove unused ms async affinity options (`pr#26099 <https://github.com/ceph/ceph/pull/26099>`_, Josh Durgin)
* core: common/util.cc: add CONTAINER_NAME processing for metadata (`pr#25383 <https://github.com/ceph/ceph/pull/25383>`_, Dan Mick)
* core: compressor: building error for QAT decompress (`pr#22609 <https://github.com/ceph/ceph/pull/22609>`_, Qiaowei Ren)
* core: crush, osd: handle multiple parents properly when applying pg upmaps (`issue#23921 <http://tracker.ceph.com/issues/23921>`_, `pr#21815 <https://github.com/ceph/ceph/pull/21815>`_, xiexingguo)
* core: erasure-code: add clay codes (`issue#19278 <http://tracker.ceph.com/issues/19278>`_, `pr#24291 <https://github.com/ceph/ceph/pull/24291>`_, Myna V, Sage Weil)
* core: erasure-code: fixes alignment issue when clay code is used with jerasure, cauchy_orig (`pr#24586 <https://github.com/ceph/ceph/pull/24586>`_, Myna)
* core: global/signal_handler.cc: report assert_file as correct name (`pr#23738 <https://github.com/ceph/ceph/pull/23738>`_, Dan Mick)
* core: include/rados: clarify which flags go where for copy_from (`pr#24497 <https://github.com/ceph/ceph/pull/24497>`_, Ilya Dryomov)
* core: include/rados.h: hide CEPH_OSDMAP_PGLOG_HARDLIMIT from ceph -s (`pr#25887 <https://github.com/ceph/ceph/pull/25887>`_, Neha Ojha)
* core: kv/KeyValueDB: Move PriCache implementation to ShardedCache (`pr#25925 <https://github.com/ceph/ceph/pull/25925>`_, Mark Nelson)
* core: kv/KeyValueDB: return const char\* from MergeOperator::name() (`issue#26875 <http://tracker.ceph.com/issues/26875>`_, `pr#23477 <https://github.com/ceph/ceph/pull/23477>`_, Sage Weil)
* core: messages/MOSDPGScan: fix initialization of query_epoch (`pr#22408 <https://github.com/ceph/ceph/pull/22408>`_, wumingqiao)
* core: mgr/balancer: add cmd to list all plans (`issue#37418 <http://tracker.ceph.com/issues/37418>`_, `pr#21937 <https://github.com/ceph/ceph/pull/21937>`_, Yang Honggang)
* core: mgr/BaseMgrModule: drop GIL for ceph_send_command (`issue#38537 <http://tracker.ceph.com/issues/38537>`_, `pr#26723 <https://github.com/ceph/ceph/pull/26723>`_, Sage Weil)
* core: mgr/MgrClient: Protect daemon_health_metrics (`issue#23352 <http://tracker.ceph.com/issues/23352>`_, `pr#23404 <https://github.com/ceph/ceph/pull/23404>`_, Kjetil Joergensen, Brad Hubbard)
* core,mgr: mon/MgrMonitor: change 'unresponsive' message to info level (`issue#24222 <http://tracker.ceph.com/issues/24222>`_, `pr#22158 <https://github.com/ceph/ceph/pull/22158>`_, Sage Weil)
* core,mgr,rbd:  mgr: generalize osd perf query and make counters accessible from modules (`pr#25114 <https://github.com/ceph/ceph/pull/25114>`_, Mykola Golub)
* core,mgr,rbd:  osd: support more dynamic perf query subkey types (`pr#25371 <https://github.com/ceph/ceph/pull/25371>`_, Mykola Golub)
* core,mgr,rbd,rgw: rgw, common: Fixes SCA issues (`pr#22007 <https://github.com/ceph/ceph/pull/22007>`_, Danny Al-Gaaf)
* core: mgr/smart: remove obsolete smart module (`pr#26411 <https://github.com/ceph/ceph/pull/26411>`_, Sage Weil)
* core: mon/LogMonitor: call no_reply() on ignored log message (`pr#22098 <https://github.com/ceph/ceph/pull/22098>`_, Sage Weil)
* core: mon/MonClient: avoid using magic number for the `MAuth::protocol` (`pr#23747 <https://github.com/ceph/ceph/pull/23747>`_, Kefu Chai)
* core: mon/MonClient: extract MonSub out (`pr#23688 <https://github.com/ceph/ceph/pull/23688>`_, Kefu Chai)
* core: mon/MonClient: use scoped_guard instead of goto (`pr#24304 <https://github.com/ceph/ceph/pull/24304>`_, Kefu Chai)
* core,mon: mon,osd: dump "compression_algorithms" in "mon metadata" (`issue#22420 <http://tracker.ceph.com/issues/22420>`_, `pr#21809 <https://github.com/ceph/ceph/pull/21809>`_, Kefu Chai, Casey Bodley)
* core,mon: mon/OSDMonitor: no_reply on MOSDFailure messages (`issue#24322 <http://tracker.ceph.com/issues/24322>`_, `pr#22259 <https://github.com/ceph/ceph/pull/22259>`_, Sage Weil)
* core,mon: mon/OSDMonitor: Warnings for expected_num_objects (`issue#24687 <http://tracker.ceph.com/issues/24687>`_, `pr#23072 <https://github.com/ceph/ceph/pull/23072>`_, Douglas Fuller)
* core: mon/OSDMonitor: two "ceph osd crush class rm" fixes (`pr#24657 <https://github.com/ceph/ceph/pull/24657>`_, xie xingguo)
* core: mon/PGMap: fix PGMapDigest decode (`pr#22066 <https://github.com/ceph/ceph/pull/22066>`_, Sage Weil)
* core: mon/PGMap: include unknown PGs in 'pg ls' (`pr#24032 <https://github.com/ceph/ceph/pull/24032>`_, Sage Weil)
* core: msg/async: do not trigger RESETSESSION from connect fault during connection phase (`issue#36612 <http://tracker.ceph.com/issues/36612>`_, `pr#25343 <https://github.com/ceph/ceph/pull/25343>`_, Sage Weil)
* core: msg/async/Event: clear time_events on shutdown (`issue#24162 <http://tracker.ceph.com/issues/24162>`_, `pr#22093 <https://github.com/ceph/ceph/pull/22093>`_, Sage Weil)
* core: msg/async: fix banner_v1 check in ProtocolV2 (`pr#26714 <https://github.com/ceph/ceph/pull/26714>`_, Yingxin Cheng)
* core: msg/async: fix include in frames_v2.h (`pr#26711 <https://github.com/ceph/ceph/pull/26711>`_, Yingxin Cheng)
* core: msg/async: fix is_queued() semantics (`pr#24693 <https://github.com/ceph/ceph/pull/24693>`_, Ilya Dryomov)
* core: msg/async: keep connection alive only actually sending (`pr#24301 <https://github.com/ceph/ceph/pull/24301>`_, Haomai Wang, Kefu Chai)
* core: os/bluestore: fix deep-scrub operation againest disk silent errors (`pr#23629 <https://github.com/ceph/ceph/pull/23629>`_, Xiaoguang Wang)
* core: os/bluestore: fix flush_commit locking (`issue#21480 <http://tracker.ceph.com/issues/21480>`_, `pr#22083 <https://github.com/ceph/ceph/pull/22083>`_, Sage Weil)
* core: OSD: add impl for filestore to get dbstatistics (`issue#24591 <http://tracker.ceph.com/issues/24591>`_, `pr#22633 <https://github.com/ceph/ceph/pull/22633>`_, lvshuhua)
* core: osdc: Change 'bool budgeted' to 'int budget' to avoid recalculating (`pr#21242 <https://github.com/ceph/ceph/pull/21242>`_, Jianpeng Ma)
* core: OSD: ceph-osd parent process need to restart log service after fork (`issue#24956 <http://tracker.ceph.com/issues/24956>`_, `pr#23090 <https://github.com/ceph/ceph/pull/23090>`_, redickwang)
* core: osdc/Objecter: fix split vs reconnect race (`issue#22544 <http://tracker.ceph.com/issues/22544>`_, `pr#23850 <https://github.com/ceph/ceph/pull/23850>`_, Sage Weil)
* core: osdc/Objecter: no need null pointer check for op->session anymore (`pr#25230 <https://github.com/ceph/ceph/pull/25230>`_, runsisi)
* core: osdc/Objecter: possible race condition with connection reset (`issue#36183 <http://tracker.ceph.com/issues/36183>`_, `pr#24276 <https://github.com/ceph/ceph/pull/24276>`_, Jason Dillaman)
* core: osdc: self-managed snapshot helper should catch decode exception (`issue#24000 <http://tracker.ceph.com/issues/24000>`_, `pr#21804 <https://github.com/ceph/ceph/pull/21804>`_, Jason Dillaman)
* core: osd, librados: add unset-manifest op (`pr#21999 <https://github.com/ceph/ceph/pull/21999>`_, Myoungwon Oh)
* core: osd,mds: make 'config rm ...' idempotent (`issue#24408 <http://tracker.ceph.com/issues/24408>`_, `pr#22395 <https://github.com/ceph/ceph/pull/22395>`_, Sage Weil)
* core: osd/mon: fix upgrades for pg log hard limit (`issue#36686 <http://tracker.ceph.com/issues/36686>`_, `pr#25816 <https://github.com/ceph/ceph/pull/25816>`_, Neha Ojha, Yuri Weinstein)
* core: osd,mon: increase mon_max_pg_per_osd to 250 (`pr#23251 <https://github.com/ceph/ceph/pull/23251>`_, Neha Ojha)
* core: osd,mon,msg: use intrusive_ptr for holding Connection::priv (`issue#20924 <http://tracker.ceph.com/issues/20924>`_, `pr#22292 <https://github.com/ceph/ceph/pull/22292>`_, Kefu Chai)
* core: osd/OSD: choose heartbeat peers more carefully (`pr#23487 <https://github.com/ceph/ceph/pull/23487>`_, xie xingguo)
* core: osd/OSD: drop extra/wrong \*unregister_pg\* (`pr#21816 <https://github.com/ceph/ceph/pull/21816>`_, xiexingguo)
* core: osd/OSDMap: be more aggressive when trying to balance (`issue#37940 <http://tracker.ceph.com/issues/37940>`_, `pr#26039 <https://github.com/ceph/ceph/pull/26039>`_, xie xingguo)
* core: osd/OSDMap: drop local pool filter in calc_pg_upmaps (`pr#26605 <https://github.com/ceph/ceph/pull/26605>`_, xie xingguo)
* core: osd/OSDMap: fix CEPHX_V2 osd requirement to nautilus, not mimic (`pr#23249 <https://github.com/ceph/ceph/pull/23249>`_, Sage Weil)
* core: osd/OSDMap: fix upmap mis-killing for erasure-coded PGs (`pr#25365 <https://github.com/ceph/ceph/pull/25365>`_, ningtao, xie xingguo)
* core: osd/OSDMap: potential access violation fix (`issue#37881 <http://tracker.ceph.com/issues/37881>`_, `pr#25930 <https://github.com/ceph/ceph/pull/25930>`_, xie xingguo)
* core: osd/OSDMap: using std::vector::reserve to reduce memory reallocation (`pr#26478 <https://github.com/ceph/ceph/pull/26478>`_, xie xingguo)
* core: osd/OSD: ping monitor if we are stuck at __waiting_for_healthy__ (`pr#23958 <https://github.com/ceph/ceph/pull/23958>`_, xie xingguo)
* core: osd/OSD: preallocate for _get_pgs/_get_pgids to avoid reallocate (`pr#25434 <https://github.com/ceph/ceph/pull/25434>`_, Jianpeng Ma)
* core: osd/PG: async-recovery should respect historical missing objects (`pr#24004 <https://github.com/ceph/ceph/pull/24004>`_, xie xingguo)
* core: osd/PG.cc: account for missing set irrespective of last_complete (`issue#37919 <http://tracker.ceph.com/issues/37919>`_, `pr#26175 <https://github.com/ceph/ceph/pull/26175>`_, Neha Ojha)
* core: osd/PG: create new PGs from activate in last_peering_reset epoch (`issue#24452 <http://tracker.ceph.com/issues/24452>`_, `pr#22478 <https://github.com/ceph/ceph/pull/22478>`_, Sage Weil)
* core: osd/PG: do not choose stray osds as async_recovery_targets (`pr#22330 <https://github.com/ceph/ceph/pull/22330>`_, Neha Ojha)
* core: osd/PG: fix misused FORCE_RECOVERY[BACKFILL] flags (`issue#27985 <http://tracker.ceph.com/issues/27985>`_, `pr#23904 <https://github.com/ceph/ceph/pull/23904>`_, xie xingguo)
* core: osd/PGLog.cc: check if complete_to points to log.end() (`pr#23450 <https://github.com/ceph/ceph/pull/23450>`_, Neha Ojha)
* core: osd/PGLog: trim - avoid dereferencing invalid iter (`pr#23546 <https://github.com/ceph/ceph/pull/23546>`_, xie xingguo)
* core: osd/PG: remove unused functions (`pr#26155 <https://github.com/ceph/ceph/pull/26155>`_, Kefu Chai)
* core: osd/PG: reset PG on osd down->up; normalize query processing (`issue#24373 <http://tracker.ceph.com/issues/24373>`_, `pr#22456 <https://github.com/ceph/ceph/pull/22456>`_, Sage Weil)
* core: osd/PG: restrict async_recovery_targets to up osds (`pr#22664 <https://github.com/ceph/ceph/pull/22664>`_, Neha Ojha)
* core: osd/PG: unset history_les_bound if local-les is used (`pr#22524 <https://github.com/ceph/ceph/pull/22524>`_, Kefu Chai)
* core: osd/PG: write pg epoch when resurrecting pg after delete vs merge race (`issue#35923 <http://tracker.ceph.com/issues/35923>`_, `pr#24061 <https://github.com/ceph/ceph/pull/24061>`_, Sage Weil)
* core: osd/PrimaryLogPG: do not count failed read in delta_stats (`pr#25687 <https://github.com/ceph/ceph/pull/25687>`_, Kefu Chai)
* core: osd/PrimaryLogPG: fix last_peering_reset checking on manifest flushing (`pr#26778 <https://github.com/ceph/ceph/pull/26778>`_, xie xingguo)
* core: osd/PrimaryLogPG: fix on_local_recover crash on stray clone (`pr#22396 <https://github.com/ceph/ceph/pull/22396>`_, Sage Weil)
* core: osd/PrimaryLogPG: fix potential pg-log overtrimming (`pr#23317 <https://github.com/ceph/ceph/pull/23317>`_, xie xingguo)
* core: osd/PrimaryLogPG: fix the extent length error of the sync read (`pr#25584 <https://github.com/ceph/ceph/pull/25584>`_, Xiaofei Cui)
* core: osd/PrimaryLogPG: fix try_flush_mark_clean write contention case (`issue#24174 <http://tracker.ceph.com/issues/24174>`_, `pr#22084 <https://github.com/ceph/ceph/pull/22084>`_, Sage Weil)
* core: osd/PrimaryLogPG: optimize recover order (`pr#23587 <https://github.com/ceph/ceph/pull/23587>`_, xie xingguo)
* core: osd/PrimaryLogPG: update missing_loc more carefully (`issue#35546 <http://tracker.ceph.com/issues/35546>`_, `pr#23895 <https://github.com/ceph/ceph/pull/23895>`_, xie xingguo)
* core: osd/ReplicatedBackend: remove useless assert (`pr#21243 <https://github.com/ceph/ceph/pull/21243>`_, Jianpeng Ma)
* core: osd/Session: fix invalid iterator dereference in Session::have_backoff() (`issue#24486 <http://tracker.ceph.com/issues/24486>`_, `pr#22497 <https://github.com/ceph/ceph/pull/22497>`_, Sage Weil)
* core:  osd: write "debug dump_missing" output to stdout (`pr#21960 <https://github.com/ceph/ceph/pull/21960>`_, Коренберг Маркr)
* core: os/kstore: support db statistic (`pr#21487 <https://github.com/ceph/ceph/pull/21487>`_, Yang Honggang)
* core: os/memstore: use ceph::mutex and friends (`pr#26026 <https://github.com/ceph/ceph/pull/26026>`_, Kefu Chai)
* core,performance:  core: avoid unnecessary refcounting of OSDMap on OSD's hot paths (`pr#24743 <https://github.com/ceph/ceph/pull/24743>`_, Radoslaw Zarzynski)
* core,performance: msg/async: avoid put message within write_lock (`pr#20731 <https://github.com/ceph/ceph/pull/20731>`_, Haomai Wang)
* core,performance: os/bluestore: make osd shard-thread do oncommits (`pr#22739 <https://github.com/ceph/ceph/pull/22739>`_, Jianpeng Ma)
* core,performance: osd/filestore: Change default filestore_merge_threshold to -10 (`issue#24686 <http://tracker.ceph.com/issues/24686>`_, `pr#22761 <https://github.com/ceph/ceph/pull/22761>`_, Douglas Fuller)
* core,performance: osd/OSDMap: map pgs with smaller batchs in calc_pg_upmaps (`pr#23734 <https://github.com/ceph/ceph/pull/23734>`_, huangjun)
* core: PG: release reservations after backfill completes (`issue#23614 <http://tracker.ceph.com/issues/23614>`_, `pr#22255 <https://github.com/ceph/ceph/pull/22255>`_, Neha Ojha)
* core: pg stuck in backfill_wait with plenty of disk space (`issue#38034 <http://tracker.ceph.com/issues/38034>`_, `pr#26375 <https://github.com/ceph/ceph/pull/26375>`_, xie xingguo, David Zafman)
* core,pybind: pybind/rados: new methods for manipulating self-managed snapshots (`pr#22579 <https://github.com/ceph/ceph/pull/22579>`_, Jason Dillaman)
* core: qa/suites/rados: minor fixes (`pr#22195 <https://github.com/ceph/ceph/pull/22195>`_, Neha Ojha)
* core: qa/suites/rados/thrash-erasure-code\*/thrashers/\*: less likely resv rejection injection (`pr#24667 <https://github.com/ceph/ceph/pull/24667>`_, Sage Weil)
* core: qa/suites/rados/thrash-old-clients: only centos and 16.04 (`pr#22106 <https://github.com/ceph/ceph/pull/22106>`_, Sage Weil)
* core: qa/suites: set osd_pg_log_dups_tracked in cfuse_workunit_suites_fsync.yaml (`pr#21909 <https://github.com/ceph/ceph/pull/21909>`_, Neha Ojha)
* core: qa/suites/upgrade/luminous-x: disable c-o-t import/export tests between versions (`issue#38294 <http://tracker.ceph.com/issues/38294>`_, `pr#27018 <https://github.com/ceph/ceph/pull/27018>`_, Sage Weil)
* core: qa/suites/upgrade/mimic-x/parallel: enable all classes (`pr#27011 <https://github.com/ceph/ceph/pull/27011>`_, Sage Weil)
* core: qa/workunits/mgr/test_localpool.sh: use new config syntax (`pr#22496 <https://github.com/ceph/ceph/pull/22496>`_, Sage Weil)
* core: qa/workunits/rados/test_health_warnings: prevent out osds (`issue#37776 <http://tracker.ceph.com/issues/37776>`_, `pr#25732 <https://github.com/ceph/ceph/pull/25732>`_, Sage Weil)
* core: rados.pyx: make all exceptions accept keyword arguments (`issue#24033 <http://tracker.ceph.com/issues/24033>`_, `pr#21853 <https://github.com/ceph/ceph/pull/21853>`_, Rishabh Dave)
* core: rados: return legacy address in 'lock info' (`pr#26150 <https://github.com/ceph/ceph/pull/26150>`_, Jason Dillaman)
* core: scrub warning check incorrectly uses mon scrub interval (`issue#37264 <http://tracker.ceph.com/issues/37264>`_, `pr#25112 <https://github.com/ceph/ceph/pull/25112>`_, David Zafman)
* core: src: no 'dne' acronym in user cmd output (`pr#21094 <https://github.com/ceph/ceph/pull/21094>`_, Gu Zhongyan)
* core,tests: Minor cleanups in tests and log output (`issue#38631 <http://tracker.ceph.com/issues/38631>`_, `issue#38678 <http://tracker.ceph.com/issues/38678>`_, `pr#26899 <https://github.com/ceph/ceph/pull/26899>`_, David Zafman)
* core,tests: qa/overrides/short_pg_log.yaml: reduce osd_{min,max}_pg_log_entries (`issue#38025 <http://tracker.ceph.com/issues/38025>`_, `pr#26101 <https://github.com/ceph/ceph/pull/26101>`_, Neha Ojha)
* core,tests: qa/suites/rados/thrash: change crush_tunables to jewel in rados_api_tests (`issue#38042 <http://tracker.ceph.com/issues/38042>`_, `pr#26122 <https://github.com/ceph/ceph/pull/26122>`_, Neha Ojha)
* core,tests: qa/suites/upgrade/luminous-x: a few fixes (`pr#22092 <https://github.com/ceph/ceph/pull/22092>`_, Sage Weil)
* core,tests: qa/tests: Set ansible-version: 2.5 (`issue#24926 <http://tracker.ceph.com/issues/24926>`_, `pr#23123 <https://github.com/ceph/ceph/pull/23123>`_, Yuri Weinstein)
* core,tests: Removal of snapshot with corrupt replica crashes osd (`issue#23875 <http://tracker.ceph.com/issues/23875>`_, `pr#22476 <https://github.com/ceph/ceph/pull/22476>`_, David Zafman)
* core,tests: test: Verify a log trim trims the dup_index (`pr#26533 <https://github.com/ceph/ceph/pull/26533>`_, Brad Hubbard)
* core,tools: osdmaptool: fix wrong test_map_pgs_dump_all output (`pr#22280 <https://github.com/ceph/ceph/pull/22280>`_, huangjun)
* core,tools: rados: provide user with more meaningful error message (`pr#26275 <https://github.com/ceph/ceph/pull/26275>`_, Mykola Golub)
* core,tools: tools/rados: allow reuse object for write test (`pr#25128 <https://github.com/ceph/ceph/pull/25128>`_, Li Wang)
* core: vstart.sh: Support SPDK in Ceph development deployment (`pr#22975 <https://github.com/ceph/ceph/pull/22975>`_, tone.zhang)
* crimson: add MonClient (`pr#23849 <https://github.com/ceph/ceph/pull/23849>`_, Kefu Chai)
* crimson: cache osdmap using LRU cache (`pr#26254 <https://github.com/ceph/ceph/pull/26254>`_, Kefu Chai, Jianpeng Ma)
* crimson/common: apply config changes also on shard.0 (`pr#23631 <https://github.com/ceph/ceph/pull/23631>`_, Yingxin)
* crimson/connection: misc changes (`pr#23044 <https://github.com/ceph/ceph/pull/23044>`_, Kefu Chai)
* crimson: crimson/mon: remove timeout support from mon::Client::authenticate() (`pr#24660 <https://github.com/ceph/ceph/pull/24660>`_, Kefu Chai)
* crimson/mon: move mon::Connection into .cc (`pr#24619 <https://github.com/ceph/ceph/pull/24619>`_, Kefu Chai)
* crimson/net: concurrent dispatch for SocketMessenger (`pr#24090 <https://github.com/ceph/ceph/pull/24090>`_, Casey Bodley)
* crimson/net: encapsulate protocol implementations with states (`pr#25176 <https://github.com/ceph/ceph/pull/25176>`_, Yingxin, Kefu Chai)
* crimson/net: encapsulate protocol implementations with states (remaining part) (`pr#25207 <https://github.com/ceph/ceph/pull/25207>`_, Yingxin)
* crimson/net: fix addresses during banner exchange (`pr#25580 <https://github.com/ceph/ceph/pull/25580>`_, Yingxin)
* crimson/net: fix compile errors in test_alien_echo.cc (`pr#24629 <https://github.com/ceph/ceph/pull/24629>`_, Yingxin)
* crimson/net: fix crimson msgr error leaks to caller (`pr#25716 <https://github.com/ceph/ceph/pull/25716>`_, Yingxin)
* crimson/net: fix misc issues for segment-fault and test-failures (`pr#25939 <https://github.com/ceph/ceph/pull/25939>`_, Yingxin Cheng, Kefu Chai)
* crimson/net: Fix racing for promise on_message (`pr#24097 <https://github.com/ceph/ceph/pull/24097>`_, Yingxin)
* crimson/net: fix unittest_seastar_messenger errors (`pr#23539 <https://github.com/ceph/ceph/pull/23539>`_, Yingxin)
* crimson/net: implement accepting/connecting states (`pr#24608 <https://github.com/ceph/ceph/pull/24608>`_, Yingxin)
* crimson/net: miscellaneous fixes to seastar-msgr (`pr#23816 <https://github.com/ceph/ceph/pull/23816>`_, Yingxin, Casey Bodley)
* crimson/net: misc fixes and features for crimson-messenger tests (`pr#26221 <https://github.com/ceph/ceph/pull/26221>`_, Yingxin Cheng)
* crimson/net: seastar-msgr refactoring (`pr#24576 <https://github.com/ceph/ceph/pull/24576>`_, Yingxin)
* crimson/net: s/repeat/keep_doing/ (`pr#23898 <https://github.com/ceph/ceph/pull/23898>`_, Kefu Chai)
* crimson/osd: add heartbeat support (`pr#26222 <https://github.com/ceph/ceph/pull/26222>`_, Kefu Chai)
* crimson/osd: add more heartbeat peers (`pr#26255 <https://github.com/ceph/ceph/pull/26255>`_, Kefu Chai)
* crimson/osd: correct the order of parameters passed to OSD::_preboot() (`pr#26774 <https://github.com/ceph/ceph/pull/26774>`_, chunmei Liu)
* crimson/osd: crimson osd driver (`pr#25304 <https://github.com/ceph/ceph/pull/25304>`_, Radoslaw Zarzynski, Kefu Chai)
* crimson/osd: remove "force_new" from ms_get_authorizer() (`pr#26054 <https://github.com/ceph/ceph/pull/26054>`_, Kefu Chai)
* crimson/osd: send known addresses at boot (`pr#26452 <https://github.com/ceph/ceph/pull/26452>`_, Kefu Chai)
* crimson: persist/load osdmap to/from store (`pr#26090 <https://github.com/ceph/ceph/pull/26090>`_, Kefu Chai)
* crimson: port messenger to seastar (`pr#22491 <https://github.com/ceph/ceph/pull/22491>`_, Kefu Chai, Casey Bodley)
* crimson/thread: add thread pool (`pr#22565 <https://github.com/ceph/ceph/pull/22565>`_, Kefu Chai)
* crimson/thread: pin thread pool to given CPU (`pr#22776 <https://github.com/ceph/ceph/pull/22776>`_, Kefu Chai)
* crush/CrushWrapper: silence compiler warning (`pr#25336 <https://github.com/ceph/ceph/pull/25336>`_, Li Wang)
* crush: fix device_class_clone for unpopulated/empty weight-sets (`issue#23386 <http://tracker.ceph.com/issues/23386>`_, `pr#22127 <https://github.com/ceph/ceph/pull/22127>`_, Sage Weil)
* crush: fix memory leak (`pr#25959 <https://github.com/ceph/ceph/pull/25959>`_, xie xingguo)
* crush: fix upmap overkill (`issue#37968 <http://tracker.ceph.com/issues/37968>`_, `pr#26179 <https://github.com/ceph/ceph/pull/26179>`_, xie xingguo)
* dashboard/mgr: Save button doesn't prevent saving an invalid form (`issue#36426 <http://tracker.ceph.com/issues/36426>`_, `pr#24577 <https://github.com/ceph/ceph/pull/24577>`_, Patrick Nawracay)
* dashboard: Return float if rate not available (`pr#22313 <https://github.com/ceph/ceph/pull/22313>`_, Boris Ranto)
* doc: add Ceph Manager Dashboard to top-level TOC (`pr#26390 <https://github.com/ceph/ceph/pull/26390>`_, Nathan Cutler)
* doc: add ceph-volume inventory sections (`pr#25092 <https://github.com/ceph/ceph/pull/25092>`_, Jan Fajerski)
* doc: add documentation for iostat (`pr#22034 <https://github.com/ceph/ceph/pull/22034>`_, Mohamad Gebai)
* doc: added demo document changes section (`pr#24791 <https://github.com/ceph/ceph/pull/24791>`_, James McClune)
* doc: added rbd default features (`pr#24720 <https://github.com/ceph/ceph/pull/24720>`_, Gaurav Sitlani)
* doc: added some Civetweb configuration options (`pr#24073 <https://github.com/ceph/ceph/pull/24073>`_, Anton Oks)
* doc: Added some hints on how to further accelerate builds with ccache (`pr#25394 <https://github.com/ceph/ceph/pull/25394>`_, Lenz Grimmer)
* doc: add instructions about using "serve-doc" to preview built document (`pr#24471 <https://github.com/ceph/ceph/pull/24471>`_, Kefu Chai)
* doc: add mds state transition diagram (`issue#22989 <http://tracker.ceph.com/issues/22989>`_, `pr#22996 <https://github.com/ceph/ceph/pull/22996>`_, Patrick Donnelly)
* doc: Add mention of ceph osd pool stats (`pr#25575 <https://github.com/ceph/ceph/pull/25575>`_, Thore Kruess)
* doc: add missing 12.2.11 release note (`pr#26596 <https://github.com/ceph/ceph/pull/26596>`_, Nathan Cutler)
* doc: add note about LVM volumes to ceph-deploy quick start (`pr#23879 <https://github.com/ceph/ceph/pull/23879>`_, David Wahler)
* doc: add release notes for 12.2.11 luminous (`pr#26228 <https://github.com/ceph/ceph/pull/26228>`_, Abhishek Lekshmanan)
* doc: add spacing to subcommand references (`pr#24669 <https://github.com/ceph/ceph/pull/24669>`_, James McClune)
* doc: add "--timeout" option to rbd-nbd (`pr#24302 <https://github.com/ceph/ceph/pull/24302>`_, Stefan Kooman)
* doc/bluestore: fix minor typos in compression section (`pr#22874 <https://github.com/ceph/ceph/pull/22874>`_, David Disseldorp)
* doc: broken link on troubleshooting-mon page (`pr#25312 <https://github.com/ceph/ceph/pull/25312>`_, James McClune)
* doc: bump up sphinx and pyyaml versions (`pr#26044 <https://github.com/ceph/ceph/pull/26044>`_, Kefu Chai)
* doc: ceph-deploy would not support --cluster option anymore (`pr#26471 <https://github.com/ceph/ceph/pull/26471>`_, Tatsuya Naganawa)
* doc: ceph: describe application subcommand in ceph man page (`pr#20645 <https://github.com/ceph/ceph/pull/20645>`_, Rishabh Dave)
* doc: ceph-iscsi-api ports should not be public facing (`pr#24248 <https://github.com/ceph/ceph/pull/24248>`_, Jason Dillaman)
* doc: ceph-volume describe better the options for migrating away from ceph-disk (`issue#24036 <http://tracker.ceph.com/issues/24036>`_, `pr#21890 <https://github.com/ceph/ceph/pull/21890>`_, Alfredo Deza)
* doc: ceph-volume dmcrypt and activate --all documentation updates (`issue#24031 <http://tracker.ceph.com/issues/24031>`_, `pr#22062 <https://github.com/ceph/ceph/pull/22062>`_, Alfredo Deza)
* doc: ceph-volume: expand on why ceph-disk was replaced (`pr#23194 <https://github.com/ceph/ceph/pull/23194>`_, Alfredo Deza)
* doc: ceph-volume: `lvm batch` documentation and man page updates (`issue#24970 <http://tracker.ceph.com/issues/24970>`_, `pr#23443 <https://github.com/ceph/ceph/pull/23443>`_, Alfredo Deza)
* doc: ceph-volume:  update batch documentation to explain filestore strategies (`issue#34309 <http://tracker.ceph.com/issues/34309>`_, `pr#23785 <https://github.com/ceph/ceph/pull/23785>`_, Alfredo Deza)
* doc: ceph-volume: zfs, the initial first submit (`pr#23674 <https://github.com/ceph/ceph/pull/23674>`_, Willem Jan Withagen)
* doc: cleaned up troubleshooting OSDs documentation (`pr#23519 <https://github.com/ceph/ceph/pull/23519>`_, James McClune)
* doc: Clean up field names in ServiceDescription and add a service field (`pr#26006 <https://github.com/ceph/ceph/pull/26006>`_, Jeff Layton)
* doc: cleanup: prune Argonaut-specific verbiage (`pr#22899 <https://github.com/ceph/ceph/pull/22899>`_, Nathan Cutler)
* doc: cleanup rendering syntax (`pr#22389 <https://github.com/ceph/ceph/pull/22389>`_, Mahati Chamarthy)
* doc: Clean up the snapshot consistency note (`pr#25655 <https://github.com/ceph/ceph/pull/25655>`_, Greg Farnum)
* doc: common,mon: add implicit `#include` headers (`pr#23930 <https://github.com/ceph/ceph/pull/23930>`_, Kefu Chai)
* doc: common/options: add description of osd objectstore backends (`issue#24147 <http://tracker.ceph.com/issues/24147>`_, `pr#22040 <https://github.com/ceph/ceph/pull/22040>`_, Alfredo Deza)
* doc: corrected options of iscsiadm command (`pr#26395 <https://github.com/ceph/ceph/pull/26395>`_, ZhuJieWen)
* doc: correct rbytes description (`pr#24966 <https://github.com/ceph/ceph/pull/24966>`_, Xiang Dai)
* doc: describe RBD QoS settings (`pr#25202 <https://github.com/ceph/ceph/pull/25202>`_, Mykola Golub)
* doc: doc/bluestore: data doesn't use two partitions (ceph-disk era) (`pr#22604 <https://github.com/ceph/ceph/pull/22604>`_, Alfredo Deza)
* doc: doc/cephfs: fixup add/remove mds docs (`pr#23836 <https://github.com/ceph/ceph/pull/23836>`_, liu wei)
* doc: doc/cephfs: remove lingering "experimental" note about multimds (`pr#22852 <https://github.com/ceph/ceph/pull/22852>`_, John Spray)
* doc: doc/dashboard: don't advise mgr_initial_modules (`pr#22808 <https://github.com/ceph/ceph/pull/22808>`_, John Spray)
* doc: doc/dashboard: fix formatting on Grafana instructions-2 (`pr#22706 <https://github.com/ceph/ceph/pull/22706>`_, Jos Collin)
* doc: doc/dashboard: fix formatting on Grafana instructions (`pr#22657 <https://github.com/ceph/ceph/pull/22657>`_, John Spray)
* doc: doc/dev/cephx_protocol: fix couple errors (`pr#23750 <https://github.com/ceph/ceph/pull/23750>`_, Kefu Chai)
* doc: doc/dev/index: update rados lead (`pr#24160 <https://github.com/ceph/ceph/pull/24160>`_, Josh Durgin)
* doc: doc/dev/msgr2.rst: update of the banner and authentication phases (`pr#20094 <https://github.com/ceph/ceph/pull/20094>`_, Ricardo Dias)
* doc: doc/dev/seastore.rst: initial draft notes (`pr#21381 <https://github.com/ceph/ceph/pull/21381>`_, Sage Weil)
* doc: doc/dev: Updated component leads table (`pr#24238 <https://github.com/ceph/ceph/pull/24238>`_, Lenz Grimmer)
* doc:  doc: fix the links in releases/schedule.rst (`pr#22364 <https://github.com/ceph/ceph/pull/22364>`_, Kefu Chai)
* doc: doc/man: mention import and export commands in rados manpage (`issue#4640 <http://tracker.ceph.com/issues/4640>`_, `pr#23186 <https://github.com/ceph/ceph/pull/23186>`_, Nathan Cutler)
* doc:  doc: Mention PURGED_SNAPDIRS and RECOVERY_DELETES in Mimic release notes (`pr#22711 <https://github.com/ceph/ceph/pull/22711>`_, Florian Haas)
* doc: doc/mgr/dashboard: fix typo in mgr ssl setup (`pr#24790 <https://github.com/ceph/ceph/pull/24790>`_, Mehdi Abaakouk)
* doc: doc/mgr: mention how to clear config setting (`pr#22157 <https://github.com/ceph/ceph/pull/22157>`_, John Spray)
* doc: doc/mgr: note need for module.py file in plugins (`pr#22622 <https://github.com/ceph/ceph/pull/22622>`_, John Spray)
* doc: doc/mgr/orchestrator: Add Architecture Image (`pr#26331 <https://github.com/ceph/ceph/pull/26331>`_, Sebastian Wagner, Kefu Chai)
* doc: doc/mgr/orchestrator: add `wal` to blink lights (`pr#25634 <https://github.com/ceph/ceph/pull/25634>`_, Sebastian Wagner)
* doc: doc/mgr/prometheus: readd section about custom instance labels (`pr#25182 <https://github.com/ceph/ceph/pull/25182>`_, Jan Fajerski)
* doc: doc/orchestrator: Aligned Documentation with specification (`pr#25893 <https://github.com/ceph/ceph/pull/25893>`_, Sebastian Wagner)
* doc: doc/orchestrator: Integrate CLI specification into the documentation (`pr#25119 <https://github.com/ceph/ceph/pull/25119>`_, Sebastian Wagner)
* doc:  doc: purge subcommand link broken (`pr#24785 <https://github.com/ceph/ceph/pull/24785>`_, James McClune)
* doc: doc/rados: Add bluestore memory autotuning docs (`pr#25069 <https://github.com/ceph/ceph/pull/25069>`_, Mark Nelson)
* doc: doc/rados/configuration: add osd scrub {begin,end} week day (`pr#25924 <https://github.com/ceph/ceph/pull/25924>`_, Neha Ojha)
* doc: doc/rados/configuration/msgr2: some documentation about msgr2 (`pr#26867 <https://github.com/ceph/ceph/pull/26867>`_, Sage Weil)
* doc: doc/rados/configuration: refresh osdmap section (`pr#26120 <https://github.com/ceph/ceph/pull/26120>`_, Ilya Dryomov)
* doc: doc/rados: correct osd path in troubleshooting-mon.rst (`pr#24964 <https://github.com/ceph/ceph/pull/24964>`_, songweibin)
* doc: doc/rados: fixed hit set type link (`pr#23833 <https://github.com/ceph/ceph/pull/23833>`_, James McClune)
* doc: doc/radosgw/s3.rst: Adding AWS S3 `Storage Class` as `Not Supported` (`pr#19571 <https://github.com/ceph/ceph/pull/19571>`_, Katie Holly)
* doc: doc/rados/operations: add balancer.rst to TOC (`pr#23684 <https://github.com/ceph/ceph/pull/23684>`_, Kefu Chai)
* doc: doc/rados/operations: add clay to erasure-code-profile (`pr#26902 <https://github.com/ceph/ceph/pull/26902>`_, Kefu Chai)
* doc: doc/rados/operations/crush-map-edits: fix 'take' syntax (`pr#24868 <https://github.com/ceph/ceph/pull/24868>`_, Remy Zandwijk, Sage Weil)
* doc: doc/rados/operations/pg-states: fix PG state names, part 2 (`pr#23165 <https://github.com/ceph/ceph/pull/23165>`_, Nathan Cutler)
* doc: doc/rados/operations/pg-states: fix PG state names (`pr#21520 <https://github.com/ceph/ceph/pull/21520>`_, Jan Fajerski)
* doc: doc/rados update invalid bash on bluestore migration (`issue#34317 <http://tracker.ceph.com/issues/34317>`_, `pr#23801 <https://github.com/ceph/ceph/pull/23801>`_, Alfredo Deza)
* doc: doc/rbd: corrected OpenStack Cinder permissions for Glance pool (`pr#22443 <https://github.com/ceph/ceph/pull/22443>`_, Jason Dillaman)
* doc: doc/rbd: explicitly state that mirroring requires connectivity to clusters (`pr#24433 <https://github.com/ceph/ceph/pull/24433>`_, Jason Dillaman)
* doc: doc/rbd/iscsi-target-cli: Update auth command (`pr#26788 <https://github.com/ceph/ceph/pull/26788>`_, Ricardo Marques)
* doc: doc/rbd/iscsi-target-cli: Update disk separator (`pr#26669 <https://github.com/ceph/ceph/pull/26669>`_, Ricardo Marques)
* doc: doc/release/luminous: v12.2.6 and v12.2.7 release notes (`pr#23057 <https://github.com/ceph/ceph/pull/23057>`_, Abhishek Lekshmanan, Sage Weil)
* doc: doc/releases: Add luminous releases 12.2.9 and 10 (`pr#25361 <https://github.com/ceph/ceph/pull/25361>`_, Brad Hubbard)
* doc: doc/releases: Add Mimic release 13.2.2 (`pr#24509 <https://github.com/ceph/ceph/pull/24509>`_, Brad Hubbard)
* doc: doc/releases: Mark Jewel EOL (`pr#23698 <https://github.com/ceph/ceph/pull/23698>`_, Brad Hubbard)
* doc: doc/releases: Mark Mimic first release as June (`pr#24099 <https://github.com/ceph/ceph/pull/24099>`_, Brad Hubbard)
* doc: doc/releases/mimic.rst: make note of 13.2.2 upgrade bug (`pr#24979 <https://github.com/ceph/ceph/pull/24979>`_, Neha Ojha)
* doc: doc/releases/mimic: tweak RBD major features (`pr#22011 <https://github.com/ceph/ceph/pull/22011>`_, Jason Dillaman)
* doc: doc/releases/mimic: Updated dashboard description (`pr#22016 <https://github.com/ceph/ceph/pull/22016>`_, Lenz Grimmer)
* doc: doc/releases/mimic: upgrade steps (`pr#21987 <https://github.com/ceph/ceph/pull/21987>`_, Sage Weil)
* doc: doc/releases/nautilus: dashboard package notes (`pr#26815 <https://github.com/ceph/ceph/pull/26815>`_, Kefu Chai)
* doc: doc/releases/schedule: Add Luminous 12.2.8 (`pr#23972 <https://github.com/ceph/ceph/pull/23972>`_, Brad Hubbard)
* doc: doc/releases/schedule: add mimic column (`pr#22006 <https://github.com/ceph/ceph/pull/22006>`_, Sage Weil)
* doc: doc/releases: Update releases to August '18 (`pr#23360 <https://github.com/ceph/ceph/pull/23360>`_, Brad Hubbard)
* doc: doc/rgw: document placement targets and storage classes (`issue#24508 <http://tracker.ceph.com/issues/24508>`_, `issue#38008 <http://tracker.ceph.com/issues/38008>`_, `pr#26997 <https://github.com/ceph/ceph/pull/26997>`_, Casey Bodley)
* doc: docs: add Clay code plugin documentation (`pr#24422 <https://github.com/ceph/ceph/pull/24422>`_, Myna)
* doc: docs: Fixed swift client authentication fail (`pr#23729 <https://github.com/ceph/ceph/pull/23729>`_, Dai Dang Van)
* doc: docs: radosgw: ldap-auth: fixed option name 'rgw_ldap_searchfilter' (`issue#23081 <http://tracker.ceph.com/issues/23081>`_, `pr#20526 <https://github.com/ceph/ceph/pull/20526>`_, Konstantin Shalygin)
* doc: doc/start: fix kube-helm.rst typo: docuiment -> document (`pr#23423 <https://github.com/ceph/ceph/pull/23423>`_, Zhou Peng)
* doc: doc/SubmittingPatches.rst: use Google style guide for doc patches (`pr#22190 <https://github.com/ceph/ceph/pull/22190>`_, Nathan Cutler)
* doc: Document correction (`pr#23926 <https://github.com/ceph/ceph/pull/23926>`_, Gangbiao Liu)
* doc: Document mappings of S3 Operations to ACL grants (`pr#26827 <https://github.com/ceph/ceph/pull/26827>`_, Adam C. Emerson)
* doc: document sizing for `block.db` (`pr#23210 <https://github.com/ceph/ceph/pull/23210>`_, Alfredo Deza)
* doc: document vstart options (`pr#22467 <https://github.com/ceph/ceph/pull/22467>`_, Mao Zhongyi)
* doc: doc/user-management: Remove obsolete reset caps command (`issue#37663 <http://tracker.ceph.com/issues/37663>`_, `pr#25550 <https://github.com/ceph/ceph/pull/25550>`_, Brad Hubbard)
* doc: edit on github (`pr#24452 <https://github.com/ceph/ceph/pull/24452>`_, Neha Ojha, Noah Watkins)
* doc: erasure-code-clay fixes typos (`pr#24653 <https://github.com/ceph/ceph/pull/24653>`_, Myna)
* doc: erasure-code-jerasure: removed default section of crush-device-class (`pr#21279 <https://github.com/ceph/ceph/pull/21279>`_, Junyoung Sung)
* doc: examples/librados: Remove not needed else clauses (`pr#24939 <https://github.com/ceph/ceph/pull/24939>`_, Marcos Paulo de Souza)
* doc: explain 'firstn v indep' in the CRUSH docs (`pr#24255 <https://github.com/ceph/ceph/pull/24255>`_, Greg Farnum)
* doc: Fix a couple typos and improve diagram formatting (`pr#23496 <https://github.com/ceph/ceph/pull/23496>`_, Bryan Stillwell)
* doc: fix a typo in doc/mgr/telegraf.rst (`pr#22267 <https://github.com/ceph/ceph/pull/22267>`_, Enming Zhang)
* doc: fix cephfs spelling errors (`pr#23763 <https://github.com/ceph/ceph/pull/23763>`_, Chen Zhenghua)
* doc: fix/cleanup freebsd osd disk creation (`pr#23600 <https://github.com/ceph/ceph/pull/23600>`_, Willem Jan Withagen)
* doc: Fix Create a Cluster url in Running Multiple Clusters (`issue#37764 <http://tracker.ceph.com/issues/37764>`_, `pr#25705 <https://github.com/ceph/ceph/pull/25705>`_, Jos Collin)
* doc: Fix EC k=3 m=2 profile overhead calculation example (`pr#20581 <https://github.com/ceph/ceph/pull/20581>`_, Charles Alva)
* doc: fixed broken urls (`pr#23564 <https://github.com/ceph/ceph/pull/23564>`_, James McClune)
* doc: fixed grammar in restore rbd image section (`pr#22944 <https://github.com/ceph/ceph/pull/22944>`_, James McClune)
* doc: fixed links in Pools section (`pr#23431 <https://github.com/ceph/ceph/pull/23431>`_, James McClune)
* doc: fixed minor typo in Debian packages section (`pr#22878 <https://github.com/ceph/ceph/pull/22878>`_, James McClune)
* doc: fixed restful mgr module SSL configuration commands (`pr#21864 <https://github.com/ceph/ceph/pull/21864>`_, Lenz Grimmer)
* doc: Fixed spelling errors in configuration section (`pr#23719 <https://github.com/ceph/ceph/pull/23719>`_, Bryan Stillwell)
* doc: Fixed syntax in iscsi initiator windows doc (`pr#25467 <https://github.com/ceph/ceph/pull/25467>`_, Michel Raabe)
* doc: Fixed the paragraph and boxes (`pr#25094 <https://github.com/ceph/ceph/pull/25094>`_, Scoots Hamilton)
* doc: Fixed the wrong numbers in mgr/dashboard.rst (`pr#22658 <https://github.com/ceph/ceph/pull/22658>`_, Jos Collin)
* doc: fixed typo in add-or-rm-mons.rst (`pr#26250 <https://github.com/ceph/ceph/pull/26250>`_, James McClune)
* doc: fixed typo in cephfs snapshots (`pr#23764 <https://github.com/ceph/ceph/pull/23764>`_, Kai Wagner)
* doc: fixed typo in CRUSH map docs (`pr#25953 <https://github.com/ceph/ceph/pull/25953>`_, James McClune)
* doc: fixed typo in man page (`pr#24792 <https://github.com/ceph/ceph/pull/24792>`_, James McClune)
* doc: Fix incorrect mention of 'osd_deep_mon_scrub_interval' (`pr#26522 <https://github.com/ceph/ceph/pull/26522>`_, Ashish Singh)
* doc: Fix iSCSI docs URL (`pr#26296 <https://github.com/ceph/ceph/pull/26296>`_, Ricardo Marques)
* doc: fix iscsi target name when configuring target (`pr#21906 <https://github.com/ceph/ceph/pull/21906>`_, Venky Shankar)
* doc: fix long description error for rgw_period_root_pool (`pr#23814 <https://github.com/ceph/ceph/pull/23814>`_, yuliyang)
* doc: fix some it's -> its typos (`pr#22802 <https://github.com/ceph/ceph/pull/22802>`_, Brad Fitzpatrick)
* doc: Fix some typos (`pr#25060 <https://github.com/ceph/ceph/pull/25060>`_, mooncake)
* doc: Fix Spelling Error In File "ceph.rst" (`pr#23917 <https://github.com/ceph/ceph/pull/23917>`_, Gangbiao Liu)
* doc: Fix Spelling Error In File dynamicresharding.rst (`pr#24175 <https://github.com/ceph/ceph/pull/24175>`_, xiaomanh)
* doc: Fix Spelling Error of Rados Deployment/Operations (`pr#23746 <https://github.com/ceph/ceph/pull/23746>`_, Li Bingyang)
* doc: Fix Spelling Error of Radosgw (`pr#23948 <https://github.com/ceph/ceph/pull/23948>`_, Li Bingyang)
* doc: Fix Spelling Error of Radosgw (`pr#24000 <https://github.com/ceph/ceph/pull/24000>`_, Li Bingyang)
* doc: Fix Spelling Error of Radosgw (`pr#24021 <https://github.com/ceph/ceph/pull/24021>`_, Li Bingyang)
* doc: Fix Spelling Error of Rados Operations (`pr#23891 <https://github.com/ceph/ceph/pull/23891>`_, Li Bingyang)
* doc: Fix Spelling Error of Rados Operations (`pr#23900 <https://github.com/ceph/ceph/pull/23900>`_, Li Bingyang)
* doc: Fix Spelling Error of Rados Operations (`pr#23903 <https://github.com/ceph/ceph/pull/23903>`_, Li Bingyang)
* doc: fix spelling errors in rbd doc (`pr#23765 <https://github.com/ceph/ceph/pull/23765>`_, Chen Zhenghua)
* doc: fix spelling errors of cephfs (`pr#23745 <https://github.com/ceph/ceph/pull/23745>`_, Chen Zhenghua)
* doc: fix the broken urls (`issue#25185 <http://tracker.ceph.com/issues/25185>`_, `pr#23310 <https://github.com/ceph/ceph/pull/23310>`_, Jos Collin)
* doc: fix the formatting of HTTP Frontends documentation (`pr#25723 <https://github.com/ceph/ceph/pull/25723>`_, James McClune)
* doc: fix typo and format issues in quick start documentation (`pr#23705 <https://github.com/ceph/ceph/pull/23705>`_, Chen Zhenghua)
* doc: fix typo in add-or-rm-mons (`pr#25661 <https://github.com/ceph/ceph/pull/25661>`_, Jos Collin)
* doc: Fix typo in ceph-fuse(8) (`pr#22214 <https://github.com/ceph/ceph/pull/22214>`_, Jos Collin)
* doc: fix typo in erasure coding example (`pr#25737 <https://github.com/ceph/ceph/pull/25737>`_, Arthur Liu)
* doc: Fix typos in Developer Guide (`pr#24067 <https://github.com/ceph/ceph/pull/24067>`_, Li Bingyang)
* doc: fix typos in doc/releases (`pr#24186 <https://github.com/ceph/ceph/pull/24186>`_, Li Bingyang)
* doc: \*/: fix typos in docs,messages,logs,comments (`pr#24139 <https://github.com/ceph/ceph/pull/24139>`_, Kefu Chai)
* doc: Fix Typos of Developer Guide (`pr#24094 <https://github.com/ceph/ceph/pull/24094>`_, Li Bingyang)
* doc: fix typos (`pr#22174 <https://github.com/ceph/ceph/pull/22174>`_, Mao Zhongyi)
* doc: .githubmap, .mailmap, .organizationmap: update contributors (`pr#24756 <https://github.com/ceph/ceph/pull/24756>`_, Tiago Melo)
* doc: githubmap, organizationmap: cleanup and add/update contributors/affiliation (`pr#22734 <https://github.com/ceph/ceph/pull/22734>`_, Tatjana Dehler)
* doc: give pool name if default pool rbd is not created (`pr#24750 <https://github.com/ceph/ceph/pull/24750>`_, Changcheng Liu)
* doc: Improve docs osd_recovery_priority, osd_recovery_op_priority and related (`pr#26705 <https://github.com/ceph/ceph/pull/26705>`_, David Zafman)
* doc: Improve OpenStack integration and multitenancy docs for radosgw (`issue#36765 <http://tracker.ceph.com/issues/36765>`_, `pr#25056 <https://github.com/ceph/ceph/pull/25056>`_, Florian Haas)
* doc: install build-doc deps without git clone (`pr#24416 <https://github.com/ceph/ceph/pull/24416>`_, Noah Watkins)
* doc: Luminous v12.2.10 release notes (`pr#25034 <https://github.com/ceph/ceph/pull/25034>`_, Nathan Cutler)
* doc: Luminous v12.2.9 release notes (`pr#24779 <https://github.com/ceph/ceph/pull/24779>`_, Nathan Cutler)
* doc: make it easier to reach the old dev doc TOC (`pr#23253 <https://github.com/ceph/ceph/pull/23253>`_, Nathan Cutler)
* doc: mention CVEs in luminous v12.2.11 release notes (`pr#26312 <https://github.com/ceph/ceph/pull/26312>`_, Nathan Cutler, Abhishek Lekshmanan)
* doc: mgr/dashboard: Add documentation about supported browsers (`issue#27207 <http://tracker.ceph.com/issues/27207>`_, `pr#23712 <https://github.com/ceph/ceph/pull/23712>`_, Tiago Melo)
* doc: mgr/dashboard: Added missing tooltip to settings icon (`pr#23935 <https://github.com/ceph/ceph/pull/23935>`_, Lenz Grimmer)
* doc: mgr/dashboard: Add hints to resolve unit test failures (`pr#23627 <https://github.com/ceph/ceph/pull/23627>`_, Stephan Müller)
* doc: mgr/dashboard: Cleaner notifications (`pr#23315 <https://github.com/ceph/ceph/pull/23315>`_, Stephan Müller)
* doc: mgr/dashboard: Cleanup of summary refresh test (`pr#25504 <https://github.com/ceph/ceph/pull/25504>`_, Stephan Müller)
* doc: mgr/dashboard: Document custom RESTController endpoints (`pr#25322 <https://github.com/ceph/ceph/pull/25322>`_, Stephan Müller)
* doc: mgr/dashboard: Fixed documentation link on RGW page (`pr#24612 <https://github.com/ceph/ceph/pull/24612>`_, Tina Kallio)
* doc: mgr/dashboard: Fix some setup steps in HACKING.rst (`pr#24788 <https://github.com/ceph/ceph/pull/24788>`_, Ranjitha G)
* doc: mgr/dashboard: Improve prettier scripts and documentation (`pr#22994 <https://github.com/ceph/ceph/pull/22994>`_, Tiago Melo)
* doc: mgr/dashboard/qa: add missing dashboard suites (`pr#25084 <https://github.com/ceph/ceph/pull/25084>`_, Tatjana Dehler)
* doc: mgr/dashboard: updated SSO documentation (`pr#25943 <https://github.com/ceph/ceph/pull/25943>`_, Alfonso Martínez)
* doc: mgr/dashboard: Update I18N documentation (`pr#25159 <https://github.com/ceph/ceph/pull/25159>`_, Tiago Melo)
* doc: mgr/orch: Fix remote_host doc reference (`issue#38254 <http://tracker.ceph.com/issues/38254>`_, `pr#26360 <https://github.com/ceph/ceph/pull/26360>`_, Ernesto Puerta)
* doc/mgr/plugins.rst: explain more about the plugin command protocol (`pr#22629 <https://github.com/ceph/ceph/pull/22629>`_, Dan Mick)
* doc: mimic is stable! (`pr#22350 <https://github.com/ceph/ceph/pull/22350>`_, Abhishek Lekshmanan)
* doc: mimic rc1 release notes (`pr#20975 <https://github.com/ceph/ceph/pull/20975>`_, Abhishek Lekshmanan)
* doc: Multiple spelling fixes (`pr#23514 <https://github.com/ceph/ceph/pull/23514>`_, Bryan Stillwell)
* doc: numbered eviction situations (`pr#24618 <https://github.com/ceph/ceph/pull/24618>`_, Scoots Hamilton)
* doc: osdmaptool/cleanup: Completed osdmaptool's usage (`issue#3214 <http://tracker.ceph.com/issues/3214>`_, `pr#13925 <https://github.com/ceph/ceph/pull/13925>`_, Vedant Nanda)
* doc: osd/PrimaryLogPG: avoid dereferencing invalid complete_to (`pr#23894 <https://github.com/ceph/ceph/pull/23894>`_, xie xingguo)
* doc: osd/PrimaryLogPG: rename list_missing -> list_unfound command (`pr#23723 <https://github.com/ceph/ceph/pull/23723>`_, xie xingguo)
* doc: PendingReleaseNotes: note newly added CLAY code (`pr#24491 <https://github.com/ceph/ceph/pull/24491>`_, Kefu Chai)
* doc: print pg peering in SVG instead of PNG (`pr#20366 <https://github.com/ceph/ceph/pull/20366>`_, Aleksei Gutikov)
* doc: Put command template into literal block (`pr#24999 <https://github.com/ceph/ceph/pull/24999>`_, Alexey Stupnikov)
* doc: qa/mgr/selftest: handle always-on module fall out (`issue#26994 <http://tracker.ceph.com/issues/26994>`_, `pr#23681 <https://github.com/ceph/ceph/pull/23681>`_, Noah Watkins)
* doc: qa: Task to emulate network delay and packet drop between two given h… (`pr#23602 <https://github.com/ceph/ceph/pull/23602>`_, Shilpa Jagannath)
* doc: qa/workunits/rbd: replace usage of 'rados rmpool' (`pr#23942 <https://github.com/ceph/ceph/pull/23942>`_, Mykola Golub)
* doc: release/mimic: correct the changelog to the latest version (`pr#22319 <https://github.com/ceph/ceph/pull/22319>`_, Abhishek Lekshmanan)
* doc: release notes for 12.2.8 luminous (`pr#23909 <https://github.com/ceph/ceph/pull/23909>`_, Abhishek Lekshmanan)
* doc: release notes for 13.2.2 mimic (`pr#24266 <https://github.com/ceph/ceph/pull/24266>`_, Abhishek Lekshmanan)
* doc: releases: mimic 13.2.1 release notes (`pr#23288 <https://github.com/ceph/ceph/pull/23288>`_, Abhishek Lekshmanan)
* doc: releases: release notes for v10.2.11 Jewel (`pr#22989 <https://github.com/ceph/ceph/pull/22989>`_, Abhishek Lekshmanan)
* doc: remove CZ mirror (`pr#21797 <https://github.com/ceph/ceph/pull/21797>`_, Tomáš Kukrál)
* doc: remove deprecated 'scrubq' from ceph(8) (`issue#35813 <http://tracker.ceph.com/issues/35813>`_, `pr#23959 <https://github.com/ceph/ceph/pull/23959>`_, Ruben Kerkhof)
* doc: remove documentation for installing google-perftools on Debian systems (`pr#22701 <https://github.com/ceph/ceph/pull/22701>`_, James McClune)
* doc: remove duplicate python packages (`pr#22203 <https://github.com/ceph/ceph/pull/22203>`_, Stefan Kooman)
* doc: Remove upstart files and references (`pr#23582 <https://github.com/ceph/ceph/pull/23582>`_, Brad Hubbard)
* doc: Remove value 'mon_osd_max_split_count' (`pr#26584 <https://github.com/ceph/ceph/pull/26584>`_, Kai Wagner)
* doc: replace rgw_namespace_expire_secs with rgw_nfs_namespace_expire_secs (`pr#20794 <https://github.com/ceph/ceph/pull/20794>`_, chnmagnus)
* doc: rewrote the iscsi-target-cli installation (`pr#23190 <https://github.com/ceph/ceph/pull/23190>`_, Massimiliano Cuttini)
* doc: rgw: fix tagging support status (`issue#24164 <http://tracker.ceph.com/issues/24164>`_, `pr#22206 <https://github.com/ceph/ceph/pull/22206>`_, Abhishek Lekshmanan)
* doc: rgw: fix the default value of usage log setting (`issue#37856 <http://tracker.ceph.com/issues/37856>`_, `pr#25892 <https://github.com/ceph/ceph/pull/25892>`_, Abhishek Lekshmanan)
* doc: Rook/orchestrator doc fixes (`pr#23472 <https://github.com/ceph/ceph/pull/23472>`_, John Spray)
* doc: s/doc/ref for dashboard urls (`pr#22772 <https://github.com/ceph/ceph/pull/22772>`_, Jos Collin)
* doc: sort releases by date and version (`pr#25972 <https://github.com/ceph/ceph/pull/25972>`_, Noah Watkins)
* doc: Spelling fixes in BlueStore config reference (`pr#23715 <https://github.com/ceph/ceph/pull/23715>`_, Bryan Stillwell)
* doc: Spelling fixes in Network config reference (`pr#23727 <https://github.com/ceph/ceph/pull/23727>`_, libingyang)
* doc: SubmittingPatches: added inline markup to important references (`pr#25978 <https://github.com/ceph/ceph/pull/25978>`_, James McClune)
* docs: update rgw info for mimic (`pr#22305 <https://github.com/ceph/ceph/pull/22305>`_, Yehuda Sadeh)
* doc: test/crimson: do not use unit.cc as the driver of unittest_seastar_denc (`pr#23937 <https://github.com/ceph/ceph/pull/23937>`_, Kefu Chai)
* doc: test/fio: Added tips for compilation of fio with 'rados' engine (`pr#24199 <https://github.com/ceph/ceph/pull/24199>`_, Adam Kupczyk)
* doc: test/msgr: add missing #include (`pr#23947 <https://github.com/ceph/ceph/pull/23947>`_, Kefu Chai)
* doc: Tidy up description wording and spelling (`pr#22599 <https://github.com/ceph/ceph/pull/22599>`_, Anthony D'Atri)
* doc: tweak RBD iSCSI docs to point to merged tooling repo (`pr#24963 <https://github.com/ceph/ceph/pull/24963>`_, Jason Dillaman)
* doc: typo fixes, s/Requered/Required/ (`pr#26406 <https://github.com/ceph/ceph/pull/26406>`_, Drunkard Zhang)
* doc: update blkin changes (`pr#22317 <https://github.com/ceph/ceph/pull/22317>`_, Mahati Chamarthy)
* doc: Update cpp.rst to accommodate the new APIs in libs3 (`pr#22162 <https://github.com/ceph/ceph/pull/22162>`_, Zhanhao Liu)
* doc: Updated Ceph Dashboard documentation (`pr#26626 <https://github.com/ceph/ceph/pull/26626>`_, Lenz Grimmer)
* doc: updated Ceph documentation links (`pr#25797 <https://github.com/ceph/ceph/pull/25797>`_, James McClune)
* doc: updated cluster map reference link (`pr#24460 <https://github.com/ceph/ceph/pull/24460>`_, James McClune)
* doc: updated crush map tunables link (`pr#24462 <https://github.com/ceph/ceph/pull/24462>`_, James McClune)
* doc: Updated dashboard documentation (features, SSL config) (`pr#22059 <https://github.com/ceph/ceph/pull/22059>`_, Lenz Grimmer)
* doc: Updated feature list and overview in dashboard.rst (`pr#26143 <https://github.com/ceph/ceph/pull/26143>`_, Lenz Grimmer)
* doc: updated get-involved.rst for ceph-dashboard (`pr#22663 <https://github.com/ceph/ceph/pull/22663>`_, Jos Collin)
* doc: Updated Mgr Dashboard documentation (`pr#24030 <https://github.com/ceph/ceph/pull/24030>`_, Lenz Grimmer)
* doc: updated multisite documentation (`issue#26997 <http://tracker.ceph.com/issues/26997>`_, `pr#23660 <https://github.com/ceph/ceph/pull/23660>`_, James McClune)
* doc: updated reference link for creating new disk offerings in cloudstack (`pr#22250 <https://github.com/ceph/ceph/pull/22250>`_, James McClune)
* doc: updated reference link for log based PG (`pr#26611 <https://github.com/ceph/ceph/pull/26611>`_, James McClune)
* doc: updated rgw multitenancy link (`pr#25929 <https://github.com/ceph/ceph/pull/25929>`_, James McClune)
* doc: updated the overview and glossary for dashboard (`pr#22750 <https://github.com/ceph/ceph/pull/22750>`_, Jos Collin)
* doc: updated wording from federated to multisite (`pr#24670 <https://github.com/ceph/ceph/pull/24670>`_, James McClune)
* doc: Update mgr/zabbix plugin documentation with link to Zabbix template (`pr#24584 <https://github.com/ceph/ceph/pull/24584>`_, Wido den Hollander)
* doc: update the description for SPDK in bluestore-config-ref.rst (`pr#22365 <https://github.com/ceph/ceph/pull/22365>`_, tone-zhang)
* doc: use :command: for subcommands in ceph-bluestore-tool manpage (`issue#24800 <http://tracker.ceph.com/issues/24800>`_, `pr#23114 <https://github.com/ceph/ceph/pull/23114>`_, Nathan Cutler)
* doc: use preferred commands for ceph config-key (`pr#26527 <https://github.com/ceph/ceph/pull/26527>`_, Changcheng Liu)
* doc: warn about how 'rados put' works in the manpage (`pr#25757 <https://github.com/ceph/ceph/pull/25757>`_, Greg Farnum)
* doc: Wip githubmap (`pr#25950 <https://github.com/ceph/ceph/pull/25950>`_, Greg Farnum)
* erasure-code,test: silence -Wunused-variable warnings (`pr#25200 <https://github.com/ceph/ceph/pull/25200>`_, Kefu Chai)
* example/librados: remove dependency on Boost system library (`issue#25054 <http://tracker.ceph.com/issues/25054>`_, `pr#23159 <https://github.com/ceph/ceph/pull/23159>`_, Nathan Cutler)
* githubmap: update contributors (`pr#22522 <https://github.com/ceph/ceph/pull/22522>`_, Kefu Chai)
* git: Ignore tags anywhere (`pr#26159 <https://github.com/ceph/ceph/pull/26159>`_, David Zafman)
* include/buffer.h: do not use ceph_assert() unless __CEPH__ is defined (`pr#23803 <https://github.com/ceph/ceph/pull/23803>`_, Kefu Chai)
* install-deps.sh: Fixes for RHEL 7 (`pr#26393 <https://github.com/ceph/ceph/pull/26393>`_, Zack Cerza)
* kv/MemDB: add perfcounter (`pr#10305 <https://github.com/ceph/ceph/pull/10305>`_, Jianpeng Ma)
* librados: add a rados_omap_iter_size function (`issue#26948 <http://tracker.ceph.com/issues/26948>`_, `pr#23593 <https://github.com/ceph/ceph/pull/23593>`_, Jeff Layton)
* librados: block MgrClient::start_command until mgrmap (`pr#21811 <https://github.com/ceph/ceph/pull/21811>`_, John Spray, Kefu Chai)
* librados: fix admin/build-doc warning (`pr#25706 <https://github.com/ceph/ceph/pull/25706>`_, Jos Collin)
* librados: fix buffer overflow for aio_exec python binding (`pr#21775 <https://github.com/ceph/ceph/pull/21775>`_, Aleksei Gutikov)
* librados: fix unitialized timeout in wait_for_osdmap (`pr#24721 <https://github.com/ceph/ceph/pull/24721>`_, Casey Bodley)
* librados: Include memory for unique_ptr definition (`issue#35833 <http://tracker.ceph.com/issues/35833>`_, `pr#23992 <https://github.com/ceph/ceph/pull/23992>`_, Brad Hubbard)
* librados: Reject the invalid pool create request at client side, rath… (`pr#21299 <https://github.com/ceph/ceph/pull/21299>`_, Yang Honggang)
* librados: return ENOENT if pool_id invalid (`pr#21609 <https://github.com/ceph/ceph/pull/21609>`_, Li Wang)
* librados: split C++ and C APIs into different source files (`pr#24616 <https://github.com/ceph/ceph/pull/24616>`_, Kefu Chai)
* librados: use ceph::async::Completion for asio bindings (`pr#21920 <https://github.com/ceph/ceph/pull/21920>`_, Casey Bodley)
* librados: use steady clock for rados_mon_op_timeout (`pr#20004 <https://github.com/ceph/ceph/pull/20004>`_, Mohamad Gebai)
* librbd: add missing shutdown states to managed lock helper (`issue#38387 <http://tracker.ceph.com/issues/38387>`_, `pr#26523 <https://github.com/ceph/ceph/pull/26523>`_, Jason Dillaman)
* librbd: add new configuration option to always move deleted items to the trash (`pr#24476 <https://github.com/ceph/ceph/pull/24476>`_, Jason Dillaman)
* librbd: add rbd image access/modified timestamps (`pr#21114 <https://github.com/ceph/ceph/pull/21114>`_, Julien Collet)
* librbd: add trash purge api calls (`pr#24427 <https://github.com/ceph/ceph/pull/24427>`_, Julien Collet, Theofilos Mouratidis, Jason Dillaman)
* librbd: always open first parent image if it exists for a snapshot (`pr#23733 <https://github.com/ceph/ceph/pull/23733>`_, Jason Dillaman)
* librbd: avoid aggregate-initializing any static_visitor (`pr#26876 <https://github.com/ceph/ceph/pull/26876>`_, Willem Jan Withagen)
* librbd: blacklisted client might not notice it lost the lock (`issue#34534 <http://tracker.ceph.com/issues/34534>`_, `pr#23829 <https://github.com/ceph/ceph/pull/23829>`_, Jason Dillaman)
* librbd: block_name_prefix is not created randomly (`issue#24634 <http://tracker.ceph.com/issues/24634>`_, `pr#22675 <https://github.com/ceph/ceph/pull/22675>`_, hyun-ha)
* librbd: bypass pool validation if "rbd_validate_pool" is false (`pr#26878 <https://github.com/ceph/ceph/pull/26878>`_, Jason Dillaman)
* librbd: commit IO as safe when complete if writeback cache is disabled (`issue#23516 <http://tracker.ceph.com/issues/23516>`_, `pr#22342 <https://github.com/ceph/ceph/pull/22342>`_, Jason Dillaman)
* librbd: corrected usage of ImageState::open flag parameter (`pr#25428 <https://github.com/ceph/ceph/pull/25428>`_, Mykola Golub)
* librbd: deep_copy: don't hide parent if zero overlap for snapshot (`issue#24545 <http://tracker.ceph.com/issues/24545>`_, `pr#22587 <https://github.com/ceph/ceph/pull/22587>`_, Mykola Golub)
* librbd: deep copy optionally support flattening cloned image (`issue#22787 <http://tracker.ceph.com/issues/22787>`_, `pr#21624 <https://github.com/ceph/ceph/pull/21624>`_, Mykola Golub)
* librbd: deep_copy: resize head object map if needed (`issue#24399 <http://tracker.ceph.com/issues/24399>`_, `pr#22415 <https://github.com/ceph/ceph/pull/22415>`_, Mykola Golub)
* librbd: deep-copy should not write to objects that cannot exist (`issue#25000 <http://tracker.ceph.com/issues/25000>`_, `pr#23132 <https://github.com/ceph/ceph/pull/23132>`_, Jason Dillaman)
* librbd: disable image mirroring when moving to trash (`pr#25509 <https://github.com/ceph/ceph/pull/25509>`_, Mykola Golub)
* librbd: disallow trash restoring when image being migrated (`pr#25529 <https://github.com/ceph/ceph/pull/25529>`_, songweibin)
* librbd: don't do create+truncate for discards with copyup (`pr#26825 <https://github.com/ceph/ceph/pull/26825>`_, Ilya Dryomov)
* librbd: ensure compare-and-write doesn't skip compare after copyup (`issue#38383 <http://tracker.ceph.com/issues/38383>`_, `pr#26519 <https://github.com/ceph/ceph/pull/26519>`_, Ilya Dryomov)
* librbd: extend API to include parent/child namespaces and image ids (`issue#36650 <http://tracker.ceph.com/issues/36650>`_, `pr#25194 <https://github.com/ceph/ceph/pull/25194>`_, Jason Dillaman)
* librbd: fix crash when opening nonexistent snapshot (`issue#24637 <http://tracker.ceph.com/issues/24637>`_, `pr#22676 <https://github.com/ceph/ceph/pull/22676>`_, Mykola Golub)
* librbd: fixed assert when flattening clone with zero overlap (`issue#35702 <http://tracker.ceph.com/issues/35702>`_, `pr#24045 <https://github.com/ceph/ceph/pull/24045>`_, Jason Dillaman)
* librbd: fix missing unblock_writes if shrink is not allowed (`issue#36778 <http://tracker.ceph.com/issues/36778>`_, `pr#25055 <https://github.com/ceph/ceph/pull/25055>`_, runsisi)
* librbd: fix possible unnecessary latency when requeue request (`pr#23815 <https://github.com/ceph/ceph/pull/23815>`_, Song Shun)
* librbd: fix potential live migration after commit issues due to not refreshed image header (`pr#23839 <https://github.com/ceph/ceph/pull/23839>`_, Mykola Golub)
* librbd: fix were_all_throttled() to avoid incorrect ret-value (`issue#38504 <http://tracker.ceph.com/issues/38504>`_, `pr#26688 <https://github.com/ceph/ceph/pull/26688>`_, Dongsheng Yang)
* librbd: flatten operation should use object map (`issue#23445 <http://tracker.ceph.com/issues/23445>`_, `pr#23941 <https://github.com/ceph/ceph/pull/23941>`_, Mykola Golub)
* librbd: force 'invalid object map' flag on-disk update (`issue#24434 <http://tracker.ceph.com/issues/24434>`_, `pr#22444 <https://github.com/ceph/ceph/pull/22444>`_, Mykola Golub)
* librbd: get_parent API method should properly handle migrating image (`issue#37998 <http://tracker.ceph.com/issues/37998>`_, `pr#26337 <https://github.com/ceph/ceph/pull/26337>`_, Jason Dillaman)
* librbd: handle aio failure in ManagedLock and PreReleaseRequest (`pr#20112 <https://github.com/ceph/ceph/pull/20112>`_, liyichao)
* librbd: improve object map performance under high IOPS workloads (`issue#38538 <http://tracker.ceph.com/issues/38538>`_, `pr#26721 <https://github.com/ceph/ceph/pull/26721>`_, Jason Dillaman)
* librbd: journaling unable request can not be sent to remote lock owner (`issue#26939 <http://tracker.ceph.com/issues/26939>`_, `pr#23649 <https://github.com/ceph/ceph/pull/23649>`_, Mykola Golub)
* librbd: keep access/modified timestamp updates out of IO path (`issue#37745 <http://tracker.ceph.com/issues/37745>`_, `pr#25883 <https://github.com/ceph/ceph/pull/25883>`_, Jason Dillaman)
* librbd: make it possible to migrate parent images (`pr#25945 <https://github.com/ceph/ceph/pull/25945>`_, Mykola Golub)
* librbd: move mirror peer attribute handling from CLI to API (`pr#25096 <https://github.com/ceph/ceph/pull/25096>`_, Jason Dillaman)
* librbd: namespace create/remove/list support (`pr#22608 <https://github.com/ceph/ceph/pull/22608>`_, Jason Dillaman)
* librbd: object copy state machine might dereference a deleted object (`issue#36220 <http://tracker.ceph.com/issues/36220>`_, `pr#24293 <https://github.com/ceph/ceph/pull/24293>`_, Jason Dillaman)
* librbd: object map improperly flagged as invalidated (`issue#24516 <http://tracker.ceph.com/issues/24516>`_, `pr#24105 <https://github.com/ceph/ceph/pull/24105>`_, Jason Dillaman)
* librbd: optionally limit journal in-flight appends (`pr#22983 <https://github.com/ceph/ceph/pull/22983>`_, Mykola Golub)
* librbd:optionally support FUA (force unit access) on write requests (`issue#19366 <http://tracker.ceph.com/issues/19366>`_, `pr#22945 <https://github.com/ceph/ceph/pull/22945>`_, ningtao)
* librbd: pool and image level config overrides (`pr#23743 <https://github.com/ceph/ceph/pull/23743>`_, Mykola Golub)
* librbd: potential object map race with copyup state machine (`issue#24516 <http://tracker.ceph.com/issues/24516>`_, `pr#24253 <https://github.com/ceph/ceph/pull/24253>`_, Jason Dillaman)
* librbd: potential race on image create request complete (`issue#24910 <http://tracker.ceph.com/issues/24910>`_, `pr#23639 <https://github.com/ceph/ceph/pull/23639>`_, Mykola Golub)
* librbd: prevent the use of internal feature bits from external users (`issue#24165 <http://tracker.ceph.com/issues/24165>`_, `pr#22072 <https://github.com/ceph/ceph/pull/22072>`_, Jason Dillaman)
* librbd: prevent use of namespaces on pre-nautilus OSDs (`pr#23823 <https://github.com/ceph/ceph/pull/23823>`_, Jason Dillaman)
* librbd: properly filter out trashed non-user images on purge (`pr#26079 <https://github.com/ceph/ceph/pull/26079>`_, Mykola Golub)
* librbd: properly handle potential object map failures (`issue#36074 <http://tracker.ceph.com/issues/36074>`_, `pr#24179 <https://github.com/ceph/ceph/pull/24179>`_, Jason Dillaman)
* librbd: race condition possible when validating RBD pool (`issue#38500 <http://tracker.ceph.com/issues/38500>`_, `pr#26683 <https://github.com/ceph/ceph/pull/26683>`_, Jason Dillaman)
* librbd: reduce the TokenBucket fill cycle and support bursting io configuration (`pr#24214 <https://github.com/ceph/ceph/pull/24214>`_, Shiyang Ruan)
* librbd: remove template declaration of a non-template function (`pr#23790 <https://github.com/ceph/ceph/pull/23790>`_, Shiyang Ruan)
* librbd: reset snaps in rbd_snap_list() (`issue#37508 <http://tracker.ceph.com/issues/37508>`_, `pr#25379 <https://github.com/ceph/ceph/pull/25379>`_, Kefu Chai)
* librbd: restart io if migration parent gone (`issue#36710 <http://tracker.ceph.com/issues/36710>`_, `pr#25175 <https://github.com/ceph/ceph/pull/25175>`_, Mykola Golub)
* librbd: send_copyup() fixes and cleanups (`pr#26483 <https://github.com/ceph/ceph/pull/26483>`_, Ilya Dryomov)
* librbd: simplify config override handling (`pr#24450 <https://github.com/ceph/ceph/pull/24450>`_, Jason Dillaman)
* librbd: skip small, unaligned discard extents by default (`issue#38146 <http://tracker.ceph.com/issues/38146>`_, `pr#26432 <https://github.com/ceph/ceph/pull/26432>`_, Jason Dillaman)
* librbd: support bps throttle and throttle read and write seperately (`pr#21635 <https://github.com/ceph/ceph/pull/21635>`_, Dongsheng Yang)
* librbd: support migrating images with minimal downtime (`issue#18430 <http://tracker.ceph.com/issues/18430>`_, `issue#24439 <http://tracker.ceph.com/issues/24439>`_, `issue#26874 <http://tracker.ceph.com/issues/26874>`_, `issue#23659 <http://tracker.ceph.com/issues/23659>`_, `pr#15831 <https://github.com/ceph/ceph/pull/15831>`_, Patrick Donnelly, Sage Weil, Alfredo Deza, Kefu Chai, Patrick Nawracay, Pavani Rajula, Mykola Golub, Casey Bodley, Yingxin, Jason Dillaman)
* librbd: support v2 cloning across namespaces (`pr#23662 <https://github.com/ceph/ceph/pull/23662>`_, Jason Dillaman)
* librbd: use object map when doing snap rollback (`pr#23110 <https://github.com/ceph/ceph/pull/23110>`_, songweibin)
* librbd: utilize the journal disabled policy when removing images (`issue#23512 <http://tracker.ceph.com/issues/23512>`_, `pr#22327 <https://github.com/ceph/ceph/pull/22327>`_, Jason Dillaman)
* librbd: validate data pool for self-managed snapshot support (`pr#22737 <https://github.com/ceph/ceph/pull/22737>`_, Mykola Golub)
* librbd: workaround an ICE of GCC (`issue#37719 <http://tracker.ceph.com/issues/37719>`_, `pr#25733 <https://github.com/ceph/ceph/pull/25733>`_, Kefu Chai)
* log: avoid heap allocations for most log entries (`pr#23721 <https://github.com/ceph/ceph/pull/23721>`_, Patrick Donnelly)
* lvm: when osd creation fails log the exception (`issue#24456 <http://tracker.ceph.com/issues/24456>`_, `pr#22627 <https://github.com/ceph/ceph/pull/22627>`_, Andrew Schoen)
* mailmap,organization: Update sangfor affiliation (`pr#25225 <https://github.com/ceph/ceph/pull/25225>`_, Zengran Zhang)
* mds: add reference when setting Connection::priv to existing session (`pr#22384 <https://github.com/ceph/ceph/pull/22384>`_, "Yan, Zheng")
* mds: fix leak of MDSCacheObject::waiting (`issue#24289 <http://tracker.ceph.com/issues/24289>`_, `pr#22307 <https://github.com/ceph/ceph/pull/22307>`_, "Yan, Zheng")
* mds: fix some memory leak (`issue#24289 <http://tracker.ceph.com/issues/24289>`_, `pr#22240 <https://github.com/ceph/ceph/pull/22240>`_, "Yan, Zheng")
* mds,messages: silence -Wclass-memaccess warnings (`pr#21845 <https://github.com/ceph/ceph/pull/21845>`_, Kefu Chai)
* mds: properly journal root inode's snaprealm (`issue#24343 <http://tracker.ceph.com/issues/24343>`_, `pr#22320 <https://github.com/ceph/ceph/pull/22320>`_, "Yan, Zheng")
* mds: remove obsolete comments (`pr#25549 <https://github.com/ceph/ceph/pull/25549>`_, Patrick Donnelly)
* mds: reply session reject for open request from blacklisted client (`pr#21941 <https://github.com/ceph/ceph/pull/21941>`_, Yan, Zheng, "Yan, Zheng")
* mgr: Add ability to trigger a cluster/audit log message from Python (`pr#24239 <https://github.com/ceph/ceph/pull/24239>`_, Volker Theile)
* mgr: Add `HandleCommandResult` namedtuple (`pr#25261 <https://github.com/ceph/ceph/pull/25261>`_, Sebastian Wagner)
* mgr: add limit param to osd perf query (`pr#25151 <https://github.com/ceph/ceph/pull/25151>`_, Mykola Golub)
* mgr: add per pool force-recovery/backfill commands (`issue#38456 <http://tracker.ceph.com/issues/38456>`_, `pr#26560 <https://github.com/ceph/ceph/pull/26560>`_, xie xingguo)
* mgr: add per pool scrub commands (`pr#26532 <https://github.com/ceph/ceph/pull/26532>`_, xie xingguo)
* mgr: Allow modules to get/set other module options (`pr#25651 <https://github.com/ceph/ceph/pull/25651>`_, Volker Theile)
* mgr: Allow rook to scale the mon count (`pr#26405 <https://github.com/ceph/ceph/pull/26405>`_, Jeff Layton)
* mgr: always on modules v2 (`pr#23970 <https://github.com/ceph/ceph/pull/23970>`_, Noah Watkins)
* mgr/ansible: Add/remove hosts (`pr#26241 <https://github.com/ceph/ceph/pull/26241>`_, Juan Miguel Olmo Martínez)
* mgr/ansible: Replace Ansible playbook used to retrieve storage devices data (`pr#26023 <https://github.com/ceph/ceph/pull/26023>`_, Juan Miguel Olmo Martínez)
* mgr/ansible: Replace deprecated <get_config> calls (`pr#25964 <https://github.com/ceph/ceph/pull/25964>`_, Juan Miguel Olmo Martínez)
* mgr: Centralize PG_STATES to MgrModule (`pr#22594 <https://github.com/ceph/ceph/pull/22594>`_, Wido den Hollander)
* mgr: ceph-mgr: hold lock while accessing the request list and submitting request (`pr#25048 <https://github.com/ceph/ceph/pull/25048>`_, Jerry Lee)
* mgr: change 'bytes' dynamic perf counters to COUNTER type (`pr#25908 <https://github.com/ceph/ceph/pull/25908>`_, Mykola Golub)
* mgr: create always on class of modules (`pr#23106 <https://github.com/ceph/ceph/pull/23106>`_, Noah Watkins)
* mgr: create shell OSD performance query class (`pr#24117 <https://github.com/ceph/ceph/pull/24117>`_, Mykola Golub)
* mgr/dashboard: About modal proposed changes (`issue#35693 <http://tracker.ceph.com/issues/35693>`_, `pr#25376 <https://github.com/ceph/ceph/pull/25376>`_, Kanika Murarka)
* mgr/dashboard: Add ability to list,set and unset cluster-wide OSD flags to the backend (`issue#24056 <http://tracker.ceph.com/issues/24056>`_, `pr#21998 <https://github.com/ceph/ceph/pull/21998>`_, Patrick Nawracay)
* mgr/dashboard: Add a 'clear filter' button to configuration page (`issue#36173 <http://tracker.ceph.com/issues/36173>`_, `pr#25712 <https://github.com/ceph/ceph/pull/25712>`_, familyuu)
* mgr/dashboard: add a script to run an API request on a rook cluster (`pr#25991 <https://github.com/ceph/ceph/pull/25991>`_, Jeff Layton)
* mgr/dashboard: Add a unit test form helper class (`pr#24633 <https://github.com/ceph/ceph/pull/24633>`_, Stephan Müller)
* mgr/dashboard: Add backend support for changing dashboard configuration settings via the REST API (`pr#22457 <https://github.com/ceph/ceph/pull/22457>`_, Patrick Nawracay)
* mgr/dashboard: Add breadcrumbs component (`issue#24781 <http://tracker.ceph.com/issues/24781>`_, `pr#23414 <https://github.com/ceph/ceph/pull/23414>`_, Tiago Melo)
* mgr/dashboard: add columns to Pools table (`pr#25791 <https://github.com/ceph/ceph/pull/25791>`_, Alfonso Martínez)
* mgr/dashboard: Add decorator to skip parameter encoding (`issue#26856 <http://tracker.ceph.com/issues/26856>`_, `pr#23419 <https://github.com/ceph/ceph/pull/23419>`_, Tiago Melo)
* mgr/dashboard: Add description to menu items on mobile navigation (`pr#26198 <https://github.com/ceph/ceph/pull/26198>`_, Sebastian Krah)
* mgr/dashboard: added command to tox.ini (`pr#26073 <https://github.com/ceph/ceph/pull/26073>`_, Alfonso Martínez)
* mgr/dashboard: added 'env_build' to 'npm run e2e' (`pr#26165 <https://github.com/ceph/ceph/pull/26165>`_, Alfonso Martínez)
* mgr/dashboard: Added new validators (`pr#22526 <https://github.com/ceph/ceph/pull/22526>`_, Stephan Müller)
* mgr/dashboard: Add error handling on the frontend (`pr#21820 <https://github.com/ceph/ceph/pull/21820>`_, Tiago Melo)
* mgr/dashboard: add Feature Toggles (`issue#37530 <http://tracker.ceph.com/issues/37530>`_, `pr#26102 <https://github.com/ceph/ceph/pull/26102>`_, Ernesto Puerta)
* mgr/dashboard: Add Filesystems list component (`pr#21913 <https://github.com/ceph/ceph/pull/21913>`_, Tiago Melo)
* mgr/dashboard: Add filtered rows number in table footer (`pr#22504 <https://github.com/ceph/ceph/pull/22504>`_, Tiago Melo)
* mgr/dashboard: Add gap between panel footer buttons (`pr#23796 <https://github.com/ceph/ceph/pull/23796>`_, Volker Theile)
* mgr/dashboard: Add guideline how to brand the UI and update the color scheme (`pr#25988 <https://github.com/ceph/ceph/pull/25988>`_, Sebastian Krah)
* mgr/dashboard: Add help menu entry (`pr#22303 <https://github.com/ceph/ceph/pull/22303>`_, Ricardo Marques)
* mgr/dashboard: Add i18n support (`pr#24803 <https://github.com/ceph/ceph/pull/24803>`_, Sebastian Krah, Tiago Melo)
* mgr/dashboard: Add implicit wait in e2e tests (`pr#26384 <https://github.com/ceph/ceph/pull/26384>`_, Tiago Melo)
* mgr/dashboard: Add info to Pools table (`pr#25489 <https://github.com/ceph/ceph/pull/25489>`_, Alfonso Martínez)
* mgr/dashboard: Add iSCSI discovery authentication UI (`pr#26320 <https://github.com/ceph/ceph/pull/26320>`_, Tiago Melo)
* mgr/dashboard: Add iSCSI Target Edit UI (`issue#38014 <http://tracker.ceph.com/issues/38014>`_, `pr#26367 <https://github.com/ceph/ceph/pull/26367>`_, Tiago Melo)
* mgr/dashboard: Add left padding to helper icon (`pr#24631 <https://github.com/ceph/ceph/pull/24631>`_, Stephan Müller)
* mgr/dashboard: Add missing frontend I18N (`issue#36719 <http://tracker.ceph.com/issues/36719>`_, `pr#25654 <https://github.com/ceph/ceph/pull/25654>`_, Tiago Melo)
* mgr/dashboard: Add missing test requirement "werkzeug" (`pr#24628 <https://github.com/ceph/ceph/pull/24628>`_, Stephan Müller)
* mgr/dashboard: Add NFS status endpoint (`issue#38399 <http://tracker.ceph.com/issues/38399>`_, `pr#26539 <https://github.com/ceph/ceph/pull/26539>`_, Tiago Melo)
* mgr/dashboard: Add 'no-unused-variable' rule to tslint (`pr#22328 <https://github.com/ceph/ceph/pull/22328>`_, Tiago Melo)
* mgr/dashboard: Add permission validation to the  "Purge Trash" button (`issue#36272 <http://tracker.ceph.com/issues/36272>`_, `pr#24370 <https://github.com/ceph/ceph/pull/24370>`_, Tiago Melo)
* mgr/dashboard: Add pool cache tiering details tab (`issue#25158 <http://tracker.ceph.com/issues/25158>`_, `pr#25602 <https://github.com/ceph/ceph/pull/25602>`_, familyuu)
* mgr/dashboard: Add Pool update endpoint (`pr#21881 <https://github.com/ceph/ceph/pull/21881>`_, Sebastian Wagner, Stephan Müller)
* mgr/dashboard: Add Prettier formatter to the frontend (`pr#21819 <https://github.com/ceph/ceph/pull/21819>`_, Tiago Melo)
* mgr/dashboard: add profiles to set cluster's rebuild performance (`pr#24968 <https://github.com/ceph/ceph/pull/24968>`_, Tatjana Dehler)
* mgr/dashboard: add pytest plugin: faulthandler (`pr#25053 <https://github.com/ceph/ceph/pull/25053>`_, Alfonso Martínez)
* mgr/dashboard: Add REST API for role management (`pr#23322 <https://github.com/ceph/ceph/pull/23322>`_, Ricardo Marques)
* mgr/dashboard: Add scrub action to the OSDs table (`pr#22122 <https://github.com/ceph/ceph/pull/22122>`_, Tiago Melo)
* mgr/dashboard: Adds custom timepicker for grafana iframes (`pr#25583 <https://github.com/ceph/ceph/pull/25583>`_, Kanika Murarka)
* mgr/dashboard: Adds ECP management to the frontend (`pr#24627 <https://github.com/ceph/ceph/pull/24627>`_, Stephan Müller)
* mgr/dashboard: Add shared Confirmation Modal (`pr#22601 <https://github.com/ceph/ceph/pull/22601>`_, Tiago Melo)
* mgr/dashboard: add supported flag information to config options documentation (`pr#22760 <https://github.com/ceph/ceph/pull/22760>`_, Tatjana Dehler)
* mgr/dashboard: Add support for iSCSI's multi backstores (UI) (`pr#26575 <https://github.com/ceph/ceph/pull/26575>`_, Tiago Melo)
* mgr/dashboard: Add support for managing individual OSD settings/characteristics in the frontend (`issue#36487 <http://tracker.ceph.com/issues/36487>`_, `issue#36444 <http://tracker.ceph.com/issues/36444>`_, `issue#35448 <http://tracker.ceph.com/issues/35448>`_, `issue#36188 <http://tracker.ceph.com/issues/36188>`_, `issue#35811 <http://tracker.ceph.com/issues/35811>`_, `issue#35816 <http://tracker.ceph.com/issues/35816>`_, `issue#36086 <http://tracker.ceph.com/issues/36086>`_, `pr#24606 <https://github.com/ceph/ceph/pull/24606>`_, Patrick Nawracay)
* mgr/dashboard: Add support for managing individual OSD settings in the backend (`issue#24270 <http://tracker.ceph.com/issues/24270>`_, `pr#23491 <https://github.com/ceph/ceph/pull/23491>`_, Patrick Nawracay)
* mgr/dashboard: Add support for managing RBD QoS (`issue#37572 <http://tracker.ceph.com/issues/37572>`_, `issue#38004 <http://tracker.ceph.com/issues/38004>`_, `issue#37570 <http://tracker.ceph.com/issues/37570>`_, `issue#37936 <http://tracker.ceph.com/issues/37936>`_, `issue#37574 <http://tracker.ceph.com/issues/37574>`_, `issue#36191 <http://tracker.ceph.com/issues/36191>`_, `issue#37845 <http://tracker.ceph.com/issues/37845>`_, `issue#37569 <http://tracker.ceph.com/issues/37569>`_, `pr#25233 <https://github.com/ceph/ceph/pull/25233>`_, Patrick Nawracay)
* mgr/dashboard: Add support for RBD Trash (`issue#24272 <http://tracker.ceph.com/issues/24272>`_, `pr#23351 <https://github.com/ceph/ceph/pull/23351>`_, Tiago Melo)
* mgr/dashboard: Add support for URI encode (`issue#24621 <http://tracker.ceph.com/issues/24621>`_, `pr#22672 <https://github.com/ceph/ceph/pull/22672>`_, Tiago Melo)
* mgr/dashboard: Add table actions component (`pr#23779 <https://github.com/ceph/ceph/pull/23779>`_, Stephan Müller)
* mgr/dashboard: Add table of contents to HACKING.rst (`pr#25812 <https://github.com/ceph/ceph/pull/25812>`_, Sebastian Krah)
* mgr/dashboard: Add token authentication to Grafana proxy (`pr#22459 <https://github.com/ceph/ceph/pull/22459>`_, Patrick Nawracay)
* mgr/dashboard: Add TSLint rule "no-unused-variable" (`pr#24699 <https://github.com/ceph/ceph/pull/24699>`_, Alfonso Martínez)
* mgr/dashboard: Add UI for Cluster-wide OSD Flags configuration (`pr#22461 <https://github.com/ceph/ceph/pull/22461>`_, Tiago Melo)
* mgr/dashboard: Add UI for disabling ACL authentication (`issue#38218 <http://tracker.ceph.com/issues/38218>`_, `pr#26388 <https://github.com/ceph/ceph/pull/26388>`_, Tiago Melo)
* mgr/dashboard: Add UI to configure the telemetry mgr plugin (`pr#25989 <https://github.com/ceph/ceph/pull/25989>`_, Volker Theile)
* mgr/dashboard: Add unique validator (`pr#23802 <https://github.com/ceph/ceph/pull/23802>`_, Volker Theile)
* mgr/dashboard: Allow "/" in pool name (`issue#38302 <http://tracker.ceph.com/issues/38302>`_, `pr#26408 <https://github.com/ceph/ceph/pull/26408>`_, Tiago Melo)
* mgr/dashboard: Allow insecure HTTPS in run-backend-api-request (`pr#21882 <https://github.com/ceph/ceph/pull/21882>`_, Sebastian Wagner)
* mgr/dashboard: Allow renaming an existing Pool (`issue#36560 <http://tracker.ceph.com/issues/36560>`_, `pr#25107 <https://github.com/ceph/ceph/pull/25107>`_, guodan1)
* mgr/dashboard: Audit REST API calls (`pr#24475 <https://github.com/ceph/ceph/pull/24475>`_, Volker Theile)
* mgr/dashboard: Auto-create a name for RBD image snapshots (`pr#23735 <https://github.com/ceph/ceph/pull/23735>`_, Volker Theile)
* mgr/dashboard: avoid blank content in Read/Write Card (`pr#25563 <https://github.com/ceph/ceph/pull/25563>`_, Alfonso Martínez)
* mgr/dashboard: awsauth: fix python3 string decode problem (`pr#21794 <https://github.com/ceph/ceph/pull/21794>`_, Ricardo Dias)
* mgr/dashboard: Can't handle user editing when tenants are specified (`pr#24757 <https://github.com/ceph/ceph/pull/24757>`_, Volker Theile)
* mgr/dashboard: Catch LookupError when checking the RGW status (`pr#24028 <https://github.com/ceph/ceph/pull/24028>`_, Volker Theile)
* mgr/dashboard: CdFormGroup (`pr#22644 <https://github.com/ceph/ceph/pull/22644>`_, Stephan Müller)
* mgr/dashboard: Ceph dashboard user management from the UI (`pr#22758 <https://github.com/ceph/ceph/pull/22758>`_, Ricardo Marques)
* mgr/dashboard: Change 'Client Recovery' title (`pr#26883 <https://github.com/ceph/ceph/pull/26883>`_, Ernesto Puerta)
* mgr/dashboard: Changed background color of Masthead to brand gray (`issue#35690 <http://tracker.ceph.com/issues/35690>`_, `pr#25628 <https://github.com/ceph/ceph/pull/25628>`_, Neha Gupta)
* mgr/dashboard: Changed default value of decimal point to 1 (`pr#22386 <https://github.com/ceph/ceph/pull/22386>`_, Tiago Melo)
* mgr/dashboard: Change icon color in notifications (`pr#26586 <https://github.com/ceph/ceph/pull/26586>`_, Volker Theile)
* mgr/dashboard: Check content-type before decode json response (`pr#24350 <https://github.com/ceph/ceph/pull/24350>`_, Ricardo Marques)
* mgr/dashboard: check for existence of Grafana dashboard (`issue#36356 <http://tracker.ceph.com/issues/36356>`_, `pr#25154 <https://github.com/ceph/ceph/pull/25154>`_, Kanika Murarka)
* mgr/dashboard: Cleanup of OSD list methods (`pr#24823 <https://github.com/ceph/ceph/pull/24823>`_, Stephan Müller)
* mgr/dashboard: Cleanup of the cluster and audit log (`pr#26188 <https://github.com/ceph/ceph/pull/26188>`_, Sebastian Krah)
* mgr/dashboard: Cleanup (`pr#24831 <https://github.com/ceph/ceph/pull/24831>`_, Patrick Nawracay)
* mgr/dashboard: Clean up pylint's `disable:no-else-return` (`pr#26509 <https://github.com/ceph/ceph/pull/26509>`_, Patrick Nawracay)
* mgr/dashboard: Cleanup Python code (`pr#26743 <https://github.com/ceph/ceph/pull/26743>`_, Volker Theile)
* mgr/dashboard: Cleanup RGW config checks (`pr#22669 <https://github.com/ceph/ceph/pull/22669>`_, Volker Theile)
* mgr/dashboard: Close modal dialogs on login screen (`pr#23328 <https://github.com/ceph/ceph/pull/23328>`_, Volker Theile)
* mgr/dashboard: code cleanup (`pr#25502 <https://github.com/ceph/ceph/pull/25502>`_, Alfonso Martínez)
* mgr/dashboard: Color variables for color codes (`issue#24575 <http://tracker.ceph.com/issues/24575>`_, `pr#22695 <https://github.com/ceph/ceph/pull/22695>`_, Kanika Murarka)
* mgr/dashboard config options add (`issue#34528 <http://tracker.ceph.com/issues/34528>`_, `issue#24996 <http://tracker.ceph.com/issues/24996>`_, `issue#24455 <http://tracker.ceph.com/issues/24455>`_, `issue#36173 <http://tracker.ceph.com/issues/36173>`_, `pr#23230 <https://github.com/ceph/ceph/pull/23230>`_, Tatjana Dehler)
* mgr/dashboard: Config options integration (read-only) depends on #22422 (`pr#21460 <https://github.com/ceph/ceph/pull/21460>`_, Tatjana Dehler)
* mgr/dashboard: config options table cleanup (`issue#34533 <http://tracker.ceph.com/issues/34533>`_, `pr#24523 <https://github.com/ceph/ceph/pull/24523>`_, Tatjana Dehler)
* mgr/dashboard: config option type names update (`issue#37843 <http://tracker.ceph.com/issues/37843>`_, `pr#25876 <https://github.com/ceph/ceph/pull/25876>`_, Tatjana Dehler)
* mgr/dashboard: configs textarea disallow horizontal resize (`issue#36452 <http://tracker.ceph.com/issues/36452>`_, `pr#24614 <https://github.com/ceph/ceph/pull/24614>`_, Tatjana Dehler)
* mgr/dashboard: Configure all mgr modules in UI (`pr#26116 <https://github.com/ceph/ceph/pull/26116>`_, Volker Theile)
* mgr/dashboard: Confirmation modal doesn't close (`pr#24544 <https://github.com/ceph/ceph/pull/24544>`_, Volker Theile)
* mgr/dashboard: Confusing tilted time stamps in the CephFS performance graph (`pr#25909 <https://github.com/ceph/ceph/pull/25909>`_, Volker Theile)
* mgr/dashboard: consider config option default values (`issue#37683 <http://tracker.ceph.com/issues/37683>`_, `pr#25616 <https://github.com/ceph/ceph/pull/25616>`_, Tatjana Dehler)
* mgr/dashboard: controller infrastructure refactor and new features (`pr#22210 <https://github.com/ceph/ceph/pull/22210>`_, Patrick Nawracay, Ricardo Dias)
* mgr/dashboard: Correct permission decorator (`pr#26135 <https://github.com/ceph/ceph/pull/26135>`_, Tina Kallio)
* mgr/dashboard: CRUSH map viewer (`issue#35684 <http://tracker.ceph.com/issues/35684>`_, `pr#24766 <https://github.com/ceph/ceph/pull/24766>`_, familyuu)
* mgr/dashboard: CRUSH map viewer RFE (`issue#37794 <http://tracker.ceph.com/issues/37794>`_, `pr#26162 <https://github.com/ceph/ceph/pull/26162>`_, familyuu)
* mgr/dashboard: Dashboard info cards refactoring (`pr#22902 <https://github.com/ceph/ceph/pull/22902>`_, Alfonso Martínez)
* mgr/dashboard: Datatable error panel blinking on page loading (`pr#23316 <https://github.com/ceph/ceph/pull/23316>`_, Volker Theile)
* mgr/dashboard: Deletion dialog falsely executes deletion when pressing 'Cancel' (`pr#22003 <https://github.com/ceph/ceph/pull/22003>`_, Volker Theile)
* mgr/dashboard: Disable package-lock.json creation (`pr#22061 <https://github.com/ceph/ceph/pull/22061>`_, Tiago Melo)
* mgr/dashboard: Disable RBD actions during task execution (`pr#23445 <https://github.com/ceph/ceph/pull/23445>`_, Ricardo Marques)
* mgr/dashboard: disallow editing read-only config options (part 2) (`pr#26450 <https://github.com/ceph/ceph/pull/26450>`_, Tatjana Dehler)
* mgr/dashboard: disallow editing read-only config options (`pr#26297 <https://github.com/ceph/ceph/pull/26297>`_, Tatjana Dehler)
* mgr/dashboard: Display logged in user (`issue#24822 <http://tracker.ceph.com/issues/24822>`_, `pr#24213 <https://github.com/ceph/ceph/pull/24213>`_, guodan1, guodan)
* mgr/dashboard: Display notification if RGW is not configured (`pr#21785 <https://github.com/ceph/ceph/pull/21785>`_, Volker Theile)
* mgr/dashboard: Display RGW user/bucket quota max size in human readable form (`pr#23842 <https://github.com/ceph/ceph/pull/23842>`_, Volker Theile)
* mgr/dashboard: Do not fetch pool list on RBD edit (`pr#22404 <https://github.com/ceph/ceph/pull/22404>`_, Ricardo Marques)
* mgr/dashboard: Do not require cert for http (`issue#36069 <http://tracker.ceph.com/issues/36069>`_, `pr#24103 <https://github.com/ceph/ceph/pull/24103>`_, Boris Ranto)
* mgr/dashboard: Drop iSCSI gateway name parameter (`pr#26984 <https://github.com/ceph/ceph/pull/26984>`_, Ricardo Marques)
* mgr/dashboard: enable coverage for API tests (`pr#26851 <https://github.com/ceph/ceph/pull/26851>`_, Alfonso Martínez)
* mgr/dashboard: Escape regex pattern in DeletionModalComponent (`issue#24902 <http://tracker.ceph.com/issues/24902>`_, `pr#23420 <https://github.com/ceph/ceph/pull/23420>`_, Tiago Melo)
* mgr/dashboard: Exception.message doesn't exist on Python 3 (`pr#24349 <https://github.com/ceph/ceph/pull/24349>`_, Ricardo Marques)
* mgr/dashboard: Extract/Refactor Task merge (`pr#23555 <https://github.com/ceph/ceph/pull/23555>`_, Stephan Müller, Tiago Melo)
* mgr/dashboard: Filter out tasks depending on permissions (`pr#25426 <https://github.com/ceph/ceph/pull/25426>`_, Tina Kallio)
* mgr/dashboard: Fix /api/grafana/validation (`pr#25997 <https://github.com/ceph/ceph/pull/25997>`_, Zack Cerza)
* mgr/dashboard: Fix bug in user form when changing password (`pr#23939 <https://github.com/ceph/ceph/pull/23939>`_, Volker Theile)
* mgr/dashboard: Fix cherrypy static content URL prefix config (`pr#23183 <https://github.com/ceph/ceph/pull/23183>`_, Ricardo Marques)
* mgr/dashboard: Fix duplicate error messages (`pr#23287 <https://github.com/ceph/ceph/pull/23287>`_, Stephan Müller)
* mgr/dashboard: Fix duplicate tasks (`pr#24930 <https://github.com/ceph/ceph/pull/24930>`_, Tiago Melo)
* mgr/dashboard: Fix e2e script (`pr#22903 <https://github.com/ceph/ceph/pull/22903>`_, Tiago Melo)
* mgr/dashboard: Fixed performance details context for host list row selection (`issue#37854 <http://tracker.ceph.com/issues/37854>`_, `pr#26020 <https://github.com/ceph/ceph/pull/26020>`_, Neha Gupta)
* mgr/dashboard: Fixed typos in environment.build.js (`pr#26650 <https://github.com/ceph/ceph/pull/26650>`_, Lenz Grimmer)
* mgr/dashboard: Fix error when clicking on newly created OSD (`issue#36245 <http://tracker.ceph.com/issues/36245>`_, `pr#24369 <https://github.com/ceph/ceph/pull/24369>`_, Patrick Nawracay)
* mgr/dashboard: Fixes documentation link- to open in new tab (`pr#22237 <https://github.com/ceph/ceph/pull/22237>`_, a2batic)
* mgr/dashboard: Fixes Grafana 500 error (`issue#37809 <http://tracker.ceph.com/issues/37809>`_, `pr#25830 <https://github.com/ceph/ceph/pull/25830>`_, Kanika Murarka)
* mgr/dashboard: Fix failing QA test: test_safe_to_destroy (`issue#37290 <http://tracker.ceph.com/issues/37290>`_, `pr#25149 <https://github.com/ceph/ceph/pull/25149>`_, Patrick Nawracay)
* mgr/dashboard: Fix flaky QA tests (`pr#24024 <https://github.com/ceph/ceph/pull/24024>`_, Patrick Nawracay)
* mgr/dashboard: Fix Forbidden Error with some roles (`issue#37293 <http://tracker.ceph.com/issues/37293>`_, `pr#25141 <https://github.com/ceph/ceph/pull/25141>`_, Ernesto Puerta)
* mgr/dashboard: fix for 'Cluster >> Hosts' page (`pr#24974 <https://github.com/ceph/ceph/pull/24974>`_, Alfonso Martínez)
* mgr/dashboard: Fix formatter service unit test (`pr#22323 <https://github.com/ceph/ceph/pull/22323>`_, Tiago Melo)
* mgr/dashboard: fix for using '::' on hosts without ipv6 (`pr#26635 <https://github.com/ceph/ceph/pull/26635>`_, Noah Watkins)
* mgr/dashboard: Fix growing table in firefox (`issue#26999 <http://tracker.ceph.com/issues/26999>`_, `pr#23711 <https://github.com/ceph/ceph/pull/23711>`_, Tiago Melo)
* mgr/dashboard: Fix HttpClient Module imports in unit tests (`pr#24679 <https://github.com/ceph/ceph/pull/24679>`_, Tiago Melo)
* mgr/dashboard: Fix iSCSI mutual password input type (`pr#26854 <https://github.com/ceph/ceph/pull/26854>`_, Ricardo Marques)
* mgr/dashboard: Fix iSCSI service unit tests (`pr#26319 <https://github.com/ceph/ceph/pull/26319>`_, Tiago Melo)
* mgr/dashboard: Fix issues in controllers/docs (`pr#26738 <https://github.com/ceph/ceph/pull/26738>`_, Volker Theile)
* mgr/dashboard: Fix Jest conflict with coverage files (`pr#22155 <https://github.com/ceph/ceph/pull/22155>`_, Tiago Melo)
* mgr/dashboard: Fix layout issues in UI (`issue#24525 <http://tracker.ceph.com/issues/24525>`_, `pr#22597 <https://github.com/ceph/ceph/pull/22597>`_, Volker Theile)
* mgr/dashboard: Fix links to external documentation (`pr#24829 <https://github.com/ceph/ceph/pull/24829>`_, Patrick Nawracay)
* mgr/dashboard: fix lint error caused by codelyzer update (`pr#22693 <https://github.com/ceph/ceph/pull/22693>`_, Tiago Melo)
* mgr/dashboard: fix lint error (`pr#22417 <https://github.com/ceph/ceph/pull/22417>`_, Tiago Melo)
* mgr/dashboard: Fix long running RBD cloning / copying message (`pr#24641 <https://github.com/ceph/ceph/pull/24641>`_, Ricardo Marques)
* mgr/dashboard: Fix missing failed restore notification (`issue#36513 <http://tracker.ceph.com/issues/36513>`_, `pr#24664 <https://github.com/ceph/ceph/pull/24664>`_, Tiago Melo)
* mgr/dashboard: Fix modified files only (frontend) (`pr#25346 <https://github.com/ceph/ceph/pull/25346>`_, Patrick Nawracay)
* mgr/dashboard: Fix moment.js deprecation warning (`pr#21981 <https://github.com/ceph/ceph/pull/21981>`_, Tiago Melo)
* mgr/dashboard: Fix more layout issues in UI (`pr#22600 <https://github.com/ceph/ceph/pull/22600>`_, Volker Theile)
* mgr/dashboard: Fix navbar focused color (`pr#25769 <https://github.com/ceph/ceph/pull/25769>`_, Volker Theile)
* mgr/dashboard: Fix notifications in user list and form (`pr#23797 <https://github.com/ceph/ceph/pull/23797>`_, Volker Theile)
* mgr/dashboard: Fix OSD down error display (`issue#24530 <http://tracker.ceph.com/issues/24530>`_, `pr#23754 <https://github.com/ceph/ceph/pull/23754>`_, Patrick Nawracay)
* mgr/dashboard: Fix pool usage not displaying on filesystem page (`pr#22453 <https://github.com/ceph/ceph/pull/22453>`_, Tiago Melo)
* mgr/dashboard: Fix problem with ErasureCodeProfileService (`pr#24694 <https://github.com/ceph/ceph/pull/24694>`_, Tiago Melo)
* mgr/dashboard: Fix Python3 issue (`pr#24617 <https://github.com/ceph/ceph/pull/24617>`_, Patrick Nawracay)
* mgr/dashboard: fix query parameters in task annotated endpoints (`issue#25096 <http://tracker.ceph.com/issues/25096>`_, `pr#23229 <https://github.com/ceph/ceph/pull/23229>`_, Ricardo Dias)
* mgr/dashboard: Fix RBD actions disable (`pr#24637 <https://github.com/ceph/ceph/pull/24637>`_, Ricardo Marques)
* mgr/dashboard: Fix RBD features style (`pr#22759 <https://github.com/ceph/ceph/pull/22759>`_, Ricardo Marques)
* mgr/dashboard: Fix RBD object size dropdown options (`pr#22830 <https://github.com/ceph/ceph/pull/22830>`_, Ricardo Marques)
* mgr/dashboard: Fix RBD task metadata (`pr#22088 <https://github.com/ceph/ceph/pull/22088>`_, Tiago Melo)
* mgr/dashboard: Fix redirect to login page on session lost (`pr#23388 <https://github.com/ceph/ceph/pull/23388>`_, Ricardo Marques)
* mgr/dashboard: fix reference to oA (`pr#24343 <https://github.com/ceph/ceph/pull/24343>`_, Joao Eduardo Luis)
* mgr/dashboard: Fix regression on rbd form component (`issue#24757 <http://tracker.ceph.com/issues/24757>`_, `pr#22829 <https://github.com/ceph/ceph/pull/22829>`_, Tiago Melo)
* mgr/dashboard: Fix reloading of pool listing (`pr#26182 <https://github.com/ceph/ceph/pull/26182>`_, Patrick Nawracay)
* mgr/dashboard: Fix renaming of pools (`pr#25423 <https://github.com/ceph/ceph/pull/25423>`_, Patrick Nawracay)
* mgr/dashboard: Fix search in `Source` column of RBD configuration list (`issue#37569 <http://tracker.ceph.com/issues/37569>`_, `pr#26765 <https://github.com/ceph/ceph/pull/26765>`_, Patrick Nawracay)
* mgr/dashboard: fix skipped backend API tests (`pr#26172 <https://github.com/ceph/ceph/pull/26172>`_, Alfonso Martínez)
* mgr/dashboard: Fix some datatable CSS issues (`pr#22216 <https://github.com/ceph/ceph/pull/22216>`_, Volker Theile)
* mgr/dashboard: Fix spaces around status labels on OSD list (`pr#24607 <https://github.com/ceph/ceph/pull/24607>`_, Patrick Nawracay)
* mgr/dashboard: Fix summary refresh call stack (`pr#25984 <https://github.com/ceph/ceph/pull/25984>`_, Tiago Melo)
* mgr/dashboard: Fix test_full_health test (`issue#37872 <http://tracker.ceph.com/issues/37872>`_, `pr#25913 <https://github.com/ceph/ceph/pull/25913>`_, Tatjana Dehler)
* mgr/dashboard: Fix test_remove_not_expired_trash qa test (`issue#37354 <http://tracker.ceph.com/issues/37354>`_, `pr#25221 <https://github.com/ceph/ceph/pull/25221>`_, Tiago Melo)
* mgr/dashboard: fix: toast notifications hiding utility menu (`pr#26429 <https://github.com/ceph/ceph/pull/26429>`_, Alfonso Martínez)
* mgr/dashboard: fix: tox not detecting deps changes (`pr#26409 <https://github.com/ceph/ceph/pull/26409>`_, Alfonso Martínez)
* mgr/dashboard: Fix ts error on iSCSI page (`pr#24715 <https://github.com/ceph/ceph/pull/24715>`_, Ricardo Marques)
* mgr/dashboard: Fix typo in NoOrchesrtatorConfiguredException class name (`pr#26334 <https://github.com/ceph/ceph/pull/26334>`_, Volker Theile)
* mgr/dashboard: Fix typo in pools management (`pr#26323 <https://github.com/ceph/ceph/pull/26323>`_, Lenz Grimmer)
* mgr/dashboard: Fix typo (`pr#23363 <https://github.com/ceph/ceph/pull/23363>`_, Volker Theile)
* mgr/dashboard: Fix unit tests cli warnings (`pr#21933 <https://github.com/ceph/ceph/pull/21933>`_, Tiago Melo)
* mgr/dashboard: Format small numbers correctly (`issue#24081 <http://tracker.ceph.com/issues/24081>`_, `pr#21980 <https://github.com/ceph/ceph/pull/21980>`_, Stephan Müller)
* mgr/dashboard: Get user ID via RGW Admin Ops API (`pr#22416 <https://github.com/ceph/ceph/pull/22416>`_, Volker Theile)
* mgr/dashboard: Grafana dashboard updates and additions (`pr#24314 <https://github.com/ceph/ceph/pull/24314>`_, Paul Cuzner)
* mgr/dashboard: Grafana graphs integration with dashboard (`pr#23666 <https://github.com/ceph/ceph/pull/23666>`_, Kanika Murarka)
* mgr/dashboard: Grafana proxy backend (`pr#21644 <https://github.com/ceph/ceph/pull/21644>`_, Patrick Nawracay)
* mgr/dashboard: Group buttons together into one menu on OSD page (`issue#37380 <http://tracker.ceph.com/issues/37380>`_, `pr#26189 <https://github.com/ceph/ceph/pull/26189>`_, Tatjana Dehler)
* mgr/dashboard: Handle class objects as regular objects in KV-table (`pr#24632 <https://github.com/ceph/ceph/pull/24632>`_, Stephan Müller)
* mgr/dashboard: Handle errors during deletion (`pr#22002 <https://github.com/ceph/ceph/pull/22002>`_, Volker Theile)
* mgr/dashboard: Hide empty fields and render all objects in KV-table (`pr#25894 <https://github.com/ceph/ceph/pull/25894>`_, Stephan Müller)
* mgr/dashboard: Hide progress bar in case of an error (`pr#22419 <https://github.com/ceph/ceph/pull/22419>`_, Volker Theile)
* mgr/dashboard: Implement OSD purge (`issue#35811 <http://tracker.ceph.com/issues/35811>`_, `pr#26242 <https://github.com/ceph/ceph/pull/26242>`_, Patrick Nawracay)
* mgr/dashboard: Improve CRUSH map viewer (`pr#24934 <https://github.com/ceph/ceph/pull/24934>`_, Volker Theile)
* mgr/dashboard: Improved support for generating OpenAPI Spec documentation (`issue#24763 <http://tracker.ceph.com/issues/24763>`_, `pr#26227 <https://github.com/ceph/ceph/pull/26227>`_, Tina Kallio)
* mgr/dashboard: Improve error message handling (`pr#24322 <https://github.com/ceph/ceph/pull/24322>`_, Volker Theile)
* mgr/dashboard: Improve error panel (`pr#21851 <https://github.com/ceph/ceph/pull/21851>`_, Volker Theile)
* mgr/dashboard: Improve exception handling in /api/rgw/status (`pr#25836 <https://github.com/ceph/ceph/pull/25836>`_, Volker Theile)
* mgr/dashboard: Improve exception handling (`issue#23823 <http://tracker.ceph.com/issues/23823>`_, `pr#21066 <https://github.com/ceph/ceph/pull/21066>`_, Sebastian Wagner)
* mgr/dashboard: Improve `HACKING.rst` (`pr#22281 <https://github.com/ceph/ceph/pull/22281>`_, Patrick Nawracay)
* mgr/dashboard: Improve 'no pool' message on rbd form (`pr#22150 <https://github.com/ceph/ceph/pull/22150>`_, Ricardo Marques)
* mgr/dashboard: Improve RBD form (`issue#38303 <http://tracker.ceph.com/issues/38303>`_, `pr#26433 <https://github.com/ceph/ceph/pull/26433>`_, Tiago Melo)
* mgr/dashboard: Improve RGW address parser (`pr#25870 <https://github.com/ceph/ceph/pull/25870>`_, Volker Theile)
* mgr/dashboard: Improve RgwUser controller (`pr#25300 <https://github.com/ceph/ceph/pull/25300>`_, Volker Theile)
* mgr/dashboard: Improves documentation for Grafana Setting (`issue#36371 <http://tracker.ceph.com/issues/36371>`_, `pr#24511 <https://github.com/ceph/ceph/pull/24511>`_, Kanika Murarka)
* mgr/dashboard: Improve str_to_bool (`pr#22757 <https://github.com/ceph/ceph/pull/22757>`_, Volker Theile)
* mgr/dashboard: Improve SummaryService and TaskWrapperService (`pr#22906 <https://github.com/ceph/ceph/pull/22906>`_, Tiago Melo)
* mgr/dashboard: Improve table pagination style (`pr#22065 <https://github.com/ceph/ceph/pull/22065>`_, Ricardo Marques)
* mgr/dashboard: Introduce pipe to convert bool to text (`pr#26507 <https://github.com/ceph/ceph/pull/26507>`_, Volker Theile)
* mgr/dashboard: iscsi: adds CLI command to enable/disable API SSL verification (`pr#26891 <https://github.com/ceph/ceph/pull/26891>`_, Ricardo Dias)
* mgr/dashboard: iSCSI - Adds support for pool/image names with dots (`pr#26503 <https://github.com/ceph/ceph/pull/26503>`_, Ricardo Marques)
* mgr/dashboard: iSCSI - Add support for disabling ACL authentication (backend) (`pr#26382 <https://github.com/ceph/ceph/pull/26382>`_, Ricardo Marques)
* mgr/dashboard: iSCSI discovery authentication API (`pr#26115 <https://github.com/ceph/ceph/pull/26115>`_, Ricardo Marques)
* mgr/dashboard: iSCSI - Infrastructure for multiple backstores (backend) (`pr#26506 <https://github.com/ceph/ceph/pull/26506>`_, Ricardo Marques)
* mgr/dashboard: iSCSI management API (`pr#25638 <https://github.com/ceph/ceph/pull/25638>`_, Ricardo Marques, Ricardo Dias)
* mgr/dashboard: iSCSI management UI (`pr#25995 <https://github.com/ceph/ceph/pull/25995>`_, Ricardo Marques, Tiago Melo)
* mgr/dashboard: iSCSI - Support iSCSI passwords with '/' (`pr#26790 <https://github.com/ceph/ceph/pull/26790>`_, Ricardo Marques)
* mgr/dashboard: JWT authentication (`pr#22833 <https://github.com/ceph/ceph/pull/22833>`_, Ricardo Dias)
* mgr/dashboard: Landing Page: chart improvements (`pr#24810 <https://github.com/ceph/ceph/pull/24810>`_, Alfonso Martínez)
* mgr/dashboard: Landing Page: info visibility (`pr#24513 <https://github.com/ceph/ceph/pull/24513>`_, Alfonso Martínez)
* mgr/dashboard: Log frontend errors + @UiController (`pr#22285 <https://github.com/ceph/ceph/pull/22285>`_, Ricardo Marques)
* mgr/dashboard: Login failure should return HTTP 400 (`pr#22403 <https://github.com/ceph/ceph/pull/22403>`_, Ricardo Marques)
* mgr/dashboard: 'Logs' links permission in Landing Page (`pr#25231 <https://github.com/ceph/ceph/pull/25231>`_, Alfonso Martínez)
* mgr/dashboard: Make deletion dialog more touch device friendly (`pr#23897 <https://github.com/ceph/ceph/pull/23897>`_, Volker Theile)
* mgr/dashboard: Map dev 'releases' to master (`pr#24763 <https://github.com/ceph/ceph/pull/24763>`_, Zack Cerza)
* mgr/dashboard: Module dashboard.services.ganesha has several lint issues (`pr#26378 <https://github.com/ceph/ceph/pull/26378>`_, Volker Theile)
* mgr/dashboard: More configs for table `updateSelectionOnRefresh` (`pr#24015 <https://github.com/ceph/ceph/pull/24015>`_, Ricardo Marques)
* mgr/dashboard: Move Cluster/Audit logs from front page to dedicated Logs page (`pr#23834 <https://github.com/ceph/ceph/pull/23834>`_, Diksha Godbole)
* mgr/dashboard: Move unit-test-helper into the new testing folder (`pr#22857 <https://github.com/ceph/ceph/pull/22857>`_, Tiago Melo)
* mgr/dashboard: Navbar dropdown button does not respond for mobile browsers (`pr#21967 <https://github.com/ceph/ceph/pull/21967>`_, Volker Theile)
* mgr/dashboard: New Landing Page: Milestone 2 (`pr#24326 <https://github.com/ceph/ceph/pull/24326>`_, Alfonso Martínez)
* mgr/dashboard: New Landing Page (`pr#23568 <https://github.com/ceph/ceph/pull/23568>`_, Alfonso Martínez)
* mgr/dashboard: nfs-ganesha: controller API documentation (`pr#26716 <https://github.com/ceph/ceph/pull/26716>`_, Ricardo Dias)
* mgr/dashboard: NFS management UI (`pr#26085 <https://github.com/ceph/ceph/pull/26085>`_, Tiago Melo)
* mgr/dashboard: ng serve bind to 0.0.0.0 (`pr#22058 <https://github.com/ceph/ceph/pull/22058>`_, Ricardo Marques)
* mgr/dashboard: no side-effects on failed user creation (`pr#24200 <https://github.com/ceph/ceph/pull/24200>`_, Joao Eduardo Luis)
* mgr/dashboard: Notification queue (`pr#25325 <https://github.com/ceph/ceph/pull/25325>`_, Stephan Müller)
* mgr/dashboard: npm run e2e:dev (`pr#25136 <https://github.com/ceph/ceph/pull/25136>`_, Stephan Müller)
* mgr/dashboard: Performance counter progress bar keeps infinitely looping (`pr#24448 <https://github.com/ceph/ceph/pull/24448>`_, Volker Theile)
* mgr/dashboard: permanent pie chart slice hiding (`pr#25276 <https://github.com/ceph/ceph/pull/25276>`_, Alfonso Martínez)
* mgr/dashboard: PGs will update as expected (`pr#26589 <https://github.com/ceph/ceph/pull/26589>`_, Stephan Müller)
* mgr/dashboard: Pool management (`pr#21614 <https://github.com/ceph/ceph/pull/21614>`_, Stephan Müller)
* mgr/dashboard: pool stats not returned by default (`pr#25635 <https://github.com/ceph/ceph/pull/25635>`_, Alfonso Martínez)
* mgr/dashboard: Possible fix for some dashboard timing issues (`issue#36107 <http://tracker.ceph.com/issues/36107>`_, `pr#24219 <https://github.com/ceph/ceph/pull/24219>`_, Patrick Nawracay)
* mgr/dashboard: Prettify package.json (`pr#22401 <https://github.com/ceph/ceph/pull/22401>`_, Ricardo Marques)
* mgr/dashboard: Prettify RGW JS code (`pr#22278 <https://github.com/ceph/ceph/pull/22278>`_, Volker Theile)
* mgr/dashboard: Prevent API call on every keystroke (`pr#23391 <https://github.com/ceph/ceph/pull/23391>`_, Volker Theile)
* mgr/dashboard: Print a blank space between value and unit (`pr#22387 <https://github.com/ceph/ceph/pull/22387>`_, Volker Theile)
* mgr/dashboard: Progress bar does not stop in TableKeyValueComponent (`pr#24016 <https://github.com/ceph/ceph/pull/24016>`_, Volker Theile)
* mgr/dashboard: Prometheus integration (`pr#25309 <https://github.com/ceph/ceph/pull/25309>`_, Stephan Müller)
* mgr/dashboard: Provide all four 'mandatory' OSD flags (`issue#37857 <http://tracker.ceph.com/issues/37857>`_, `pr#25905 <https://github.com/ceph/ceph/pull/25905>`_, Tatjana Dehler)
* mgr/dashboard/qa: Fix ECP creation test (`pr#25120 <https://github.com/ceph/ceph/pull/25120>`_, Stephan Müller)
* mgr/dashboard/qa: Fix various vstart_runner.py issues (`issue#36581 <http://tracker.ceph.com/issues/36581>`_, `pr#24767 <https://github.com/ceph/ceph/pull/24767>`_, Volker Theile)
* mgr/dashboard: Redirect /block to /block/rbd (`pr#24722 <https://github.com/ceph/ceph/pull/24722>`_, Zack Cerza)
* mgr/dashboard: Reduce Jest logs in CI (`pr#24764 <https://github.com/ceph/ceph/pull/24764>`_, Tiago Melo)
* mgr/dashboard: Refactor autofocus directive (`pr#23910 <https://github.com/ceph/ceph/pull/23910>`_, Volker Theile)
* mgr/dashboard: Refactoring of `DeletionModalComponent` (`pr#24005 <https://github.com/ceph/ceph/pull/24005>`_, Patrick Nawracay)
* mgr/dashboard: Refactor perf counters (`pr#21673 <https://github.com/ceph/ceph/pull/21673>`_, Volker Theile)
* mgr/dashboard: Refactor RGW backend (`pr#21784 <https://github.com/ceph/ceph/pull/21784>`_, Volker Theile)
* mgr/dashboard: Refactor role management (`pr#23960 <https://github.com/ceph/ceph/pull/23960>`_, Volker Theile)
* mgr/dashboard: Relocate empty pipe (`pr#26588 <https://github.com/ceph/ceph/pull/26588>`_, Volker Theile)
* mgr/dashboard: Removed unnecessary fake services from unit tests (`pr#22473 <https://github.com/ceph/ceph/pull/22473>`_, Stephan Müller)
* mgr/dashboard: Remove fieldsets when using CdTable (`pr#23730 <https://github.com/ceph/ceph/pull/23730>`_, Tiago Melo)
* mgr/dashboard: Remove _filterValue from CdFormGroup (`issue#26861 <http://tracker.ceph.com/issues/26861>`_, `pr#24719 <https://github.com/ceph/ceph/pull/24719>`_, Stephan Müller)
* mgr/dashboard: Remove husky package (`pr#21971 <https://github.com/ceph/ceph/pull/21971>`_, Tiago Melo)
* mgr/dashboard: Remove karma packages (`pr#23181 <https://github.com/ceph/ceph/pull/23181>`_, Tiago Melo)
* mgr/dashboard: Remove param when calling notificationService.show (`pr#26447 <https://github.com/ceph/ceph/pull/26447>`_, Volker Theile)
* mgr/dashboard: Remove top-right actions text and add "About" page (`pr#22762 <https://github.com/ceph/ceph/pull/22762>`_, Ricardo Marques)
* mgr/dashboard: Remove unused code (`pr#25439 <https://github.com/ceph/ceph/pull/25439>`_, Patrick Nawracay)
* mgr/dashboard: Remove useless code (`pr#23911 <https://github.com/ceph/ceph/pull/23911>`_, Volker Theile)
* mgr/dashboard: Remove useless observable unsubscriptions (`pr#21928 <https://github.com/ceph/ceph/pull/21928>`_, Ricardo Marques)
* mgr/dashboard: replace configuration html table with cd-table (`pr#21643 <https://github.com/ceph/ceph/pull/21643>`_, Tatjana Dehler)
* mgr/dashboard: Replaced "Pool" with "Pools" in navigation bar (`pr#22715 <https://github.com/ceph/ceph/pull/22715>`_, Lenz Grimmer)
* mgr/dashboard: Replace RGW proxy controller (`issue#24436 <http://tracker.ceph.com/issues/24436>`_, `pr#22470 <https://github.com/ceph/ceph/pull/22470>`_, Volker Theile)
* mgr/dashboard: Reset settings to their default values (`pr#22298 <https://github.com/ceph/ceph/pull/22298>`_, Patrick Nawracay)
* mgr/dashboard: Resolve TestBed performance issue (`pr#21783 <https://github.com/ceph/ceph/pull/21783>`_, Stephan Müller)
* mgr/dashboard: rest: add support for query params (`pr#22318 <https://github.com/ceph/ceph/pull/22318>`_, Ricardo Dias)
* mgr/dashboard: RestClient can't handle ProtocolError exceptions (`pr#23347 <https://github.com/ceph/ceph/pull/23347>`_, Volker Theile)
* mgr/dashboard: restcontroller: minor improvements and bug fixes (`pr#22528 <https://github.com/ceph/ceph/pull/22528>`_, Ricardo Dias)
* mgr/dashboard: RGW is not working if an URL prefix is defined (`pr#23200 <https://github.com/ceph/ceph/pull/23200>`_, Volker Theile)
* mgr/dashboard: RGW proxy can't handle self-signed SSL certificates (`pr#22735 <https://github.com/ceph/ceph/pull/22735>`_, Volker Theile)
* mgr/dashboard: role based authentication/authorization system (`issue#23796 <http://tracker.ceph.com/issues/23796>`_, `pr#22283 <https://github.com/ceph/ceph/pull/22283>`_, Ricardo Marques, Ricardo Dias)
* mgr/dashboard: Role management from the UI (`pr#23409 <https://github.com/ceph/ceph/pull/23409>`_, Ricardo Marques)
* mgr/dashboard: Search broken for entries with null values (`issue#38583 <http://tracker.ceph.com/issues/38583>`_, `pr#26766 <https://github.com/ceph/ceph/pull/26766>`_, Patrick Nawracay)
* mgr/dashboard: set errno via the parent class (`pr#21945 <https://github.com/ceph/ceph/pull/21945>`_, Kefu Chai, Ricardo Dias)
* mgr/dashboard: Set MODULE_OPTIONS types and defaults (`pr#26386 <https://github.com/ceph/ceph/pull/26386>`_, Volker Theile)
* mgr/dashboard: Set timeout in RestClient calls (`pr#23224 <https://github.com/ceph/ceph/pull/23224>`_, Volker Theile)
* mgr/dashboard: Settings service (`pr#25327 <https://github.com/ceph/ceph/pull/25327>`_, Stephan Müller)
* mgr/dashboard: Show/Hide Grafana tabs according to user role (`issue#36655 <http://tracker.ceph.com/issues/36655>`_, `pr#24851 <https://github.com/ceph/ceph/pull/24851>`_, Kanika Murarka)
* mgr/dashboard: Show pool dropdown for block-mgr (`issue#37295 <http://tracker.ceph.com/issues/37295>`_, `pr#25144 <https://github.com/ceph/ceph/pull/25144>`_, Ernesto Puerta)
* mgr/dashboard: Show success notification in RGW forms (`pr#26482 <https://github.com/ceph/ceph/pull/26482>`_, Volker Theile)
* mgr/dashboard: Simplification of PoolForm method (`pr#24892 <https://github.com/ceph/ceph/pull/24892>`_, Patrick Nawracay)
* mgr/dashboard: Simplify OSD disabled action test (`pr#24824 <https://github.com/ceph/ceph/pull/24824>`_, Stephan Müller)
* mgr/dashboard: special casing for minikube in run-backend-rook-api-request.sh (`pr#26600 <https://github.com/ceph/ceph/pull/26600>`_, Jeff Layton)
* mgr/dashboard: SSO - SAML 2.0 support (`pr#24489 <https://github.com/ceph/ceph/pull/24489>`_, Ricardo Marques, Ricardo Dias)
* mgr/dashboard: SSO - UserDoesNotExist page (`pr#26058 <https://github.com/ceph/ceph/pull/26058>`_, Alfonso Martínez)
* mgr/dashboard: Stacktrace is optional on 'js-error' endpoint (`pr#22402 <https://github.com/ceph/ceph/pull/22402>`_, Ricardo Marques)
* mgr/dashboard: Status info cards' improvements (`pr#25155 <https://github.com/ceph/ceph/pull/25155>`_, Alfonso Martínez)
* mgr/dashboard: Store user table configurations (`pr#20822 <https://github.com/ceph/ceph/pull/20822>`_, Stephan Müller)
* mgr/dashboard: Stringify object[] in KV-table (`pr#22422 <https://github.com/ceph/ceph/pull/22422>`_, Stephan Müller)
* mgr/dashboard: Swagger-UI based Dashboard REST API page (`issue#23898 <http://tracker.ceph.com/issues/23898>`_, `pr#22282 <https://github.com/ceph/ceph/pull/22282>`_, Ricardo Dias)
* mgr/dashboard: Sync column style with the rest of the UI (`pr#26407 <https://github.com/ceph/ceph/pull/26407>`_, Volker Theile)
* mgr/dashboard: tasks.mgr.dashboard.test_osd.OsdTest failures (`pr#24947 <https://github.com/ceph/ceph/pull/24947>`_, Volker Theile)
* mgr/dashboard: Task wrapper service (`pr#22014 <https://github.com/ceph/ceph/pull/22014>`_, Stephan Müller)
* mgr/dashboard: The RGW backend doesn't handle IPv6 properly (`pr#24222 <https://github.com/ceph/ceph/pull/24222>`_, Volker Theile)
* mgr/dashboard: typescript cleanup (`pr#26338 <https://github.com/ceph/ceph/pull/26338>`_, Alfonso Martínez)
* mgr/dashboard: Unit Tests cleanup (`pr#24591 <https://github.com/ceph/ceph/pull/24591>`_, Tiago Melo)
* mgr/dashboard: Update Angular packages (`pr#23706 <https://github.com/ceph/ceph/pull/23706>`_, Tiago Melo)
* mgr/dashboard: Update Angular to version 6 (`pr#22082 <https://github.com/ceph/ceph/pull/22082>`_, Tiago Melo)
* mgr/dashboard: Update bootstrap to v3.4.1 (`pr#26410 <https://github.com/ceph/ceph/pull/26410>`_, Tiago Melo)
* mgr/dashboard: Updated colors in PG Status chart (`pr#26203 <https://github.com/ceph/ceph/pull/26203>`_, Alfonso Martínez)
* mgr/dashboard: updated health API test (`pr#25813 <https://github.com/ceph/ceph/pull/25813>`_, Alfonso Martínez)
* mgr/dashboard: Updated image on 404 page (`pr#23820 <https://github.com/ceph/ceph/pull/23820>`_, Lenz Grimmer)
* mgr/dashboard: Update frontend packages (`pr#23466 <https://github.com/ceph/ceph/pull/23466>`_, Tiago Melo)
* mgr/dashboard: Update I18N translation (`pr#26649 <https://github.com/ceph/ceph/pull/26649>`_, Tiago Melo)
* mgr/dashboard: Update npm packages (`pr#24681 <https://github.com/ceph/ceph/pull/24681>`_, Tiago Melo)
* mgr/dashboard: Update npm packages (`pr#25656 <https://github.com/ceph/ceph/pull/25656>`_, Tiago Melo)
* mgr/dashboard: Update npm packages (`pr#26437 <https://github.com/ceph/ceph/pull/26437>`_, Tiago Melo)
* mgr/dashboard: Update npm packages (`pr#26647 <https://github.com/ceph/ceph/pull/26647>`_, Tiago Melo)
* mgr/dashboard: update python dependency (`pr#24928 <https://github.com/ceph/ceph/pull/24928>`_, Alfonso Martínez)
* mgr/dashboard: Update RxJS to version 6 (`pr#21826 <https://github.com/ceph/ceph/pull/21826>`_, Tiago Melo)
* mgr/dashboard: upgraded python dev dependencies (`pr#26007 <https://github.com/ceph/ceph/pull/26007>`_, Alfonso Martínez)
* mgr/dashboard: Upgrade Swimlane's data-table (`pr#21880 <https://github.com/ceph/ceph/pull/21880>`_, Volker Theile)
* mgr/dashboard: Use HTTPS in dev proxy configuration and HACKING.rst (`pr#21777 <https://github.com/ceph/ceph/pull/21777>`_, Volker Theile)
* mgr/dashboard: Use human readable units on the sparkline graphs (`issue#25075 <http://tracker.ceph.com/issues/25075>`_, `pr#23446 <https://github.com/ceph/ceph/pull/23446>`_, Tiago Melo)
* mgr/dashboard: User password should be optional (`pr#24128 <https://github.com/ceph/ceph/pull/24128>`_, Ricardo Marques)
* mgr/dashboard: Validate the OSD recovery priority form input values (`issue#37436 <http://tracker.ceph.com/issues/37436>`_, `pr#25472 <https://github.com/ceph/ceph/pull/25472>`_, Tatjana Dehler)
* mgr/dashboard: Validation for duplicate RGW user email (`issue#37369 <http://tracker.ceph.com/issues/37369>`_, `pr#25334 <https://github.com/ceph/ceph/pull/25334>`_, Kanika Murarka)
* mgr: define option defaults for MgrStandbyModule as well (`pr#25734 <https://github.com/ceph/ceph/pull/25734>`_, Kefu Chai)
* mgr: devicehealth: dont error on dict iteritems (`pr#22827 <https://github.com/ceph/ceph/pull/22827>`_, Abhishek Lekshmanan)
* mgr: Diskprediction cloud activate when config changes (`pr#25165 <https://github.com/ceph/ceph/pull/25165>`_, Rick Chen)
* mgr: don't write to output if EOPNOTSUPP (`issue#37444 <http://tracker.ceph.com/issues/37444>`_, `pr#25317 <https://github.com/ceph/ceph/pull/25317>`_, Kefu Chai)
* mgr: enable inter-module calls (`pr#22951 <https://github.com/ceph/ceph/pull/22951>`_, John Spray)
* mgr: Expose avgcount to the python modules (`pr#22010 <https://github.com/ceph/ceph/pull/22010>`_, Boris Ranto)
* mgr: expose avg data for long running avgs (`pr#22420 <https://github.com/ceph/ceph/pull/22420>`_, Boris Ranto)
* mgr: expose ec profiles through manager (`pr#23010 <https://github.com/ceph/ceph/pull/23010>`_, Noah Watkins)
* mgr: Extend batch to accept explicit device lists (`issue#37502 <http://tracker.ceph.com/issues/37502>`_, `issue#37086 <http://tracker.ceph.com/issues/37086>`_, `issue#37590 <http://tracker.ceph.com/issues/37590>`_, `pr#25542 <https://github.com/ceph/ceph/pull/25542>`_, Jan Fajerski)
* mgr: fix beacon interruption caused by deadlock (`pr#23482 <https://github.com/ceph/ceph/pull/23482>`_, Yan Jun)
* mgr: fix crash due to multiple sessions from daemons with same name (`pr#25534 <https://github.com/ceph/ceph/pull/25534>`_, Mykola Golub)
* mgr: fix permissions on `balancer execute` (`issue#25345 <http://tracker.ceph.com/issues/25345>`_, `pr#23387 <https://github.com/ceph/ceph/pull/23387>`_, John Spray)
* mgr: Fix rook spec and have service_describe provide rados_config_location field for nfs services (`pr#25970 <https://github.com/ceph/ceph/pull/25970>`_, Jeff Layton)
* mgr: fix typo in variable name and cleanups (`pr#22069 <https://github.com/ceph/ceph/pull/22069>`_, Kefu Chai)
* mgr: fixup pgs show in unknown state (`issue#25103 <http://tracker.ceph.com/issues/25103>`_, `pr#23622 <https://github.com/ceph/ceph/pull/23622>`_, huanwen ren)
* mgr: Ignore daemon if no metadata was returned (`pr#22794 <https://github.com/ceph/ceph/pull/22794>`_, Wido den Hollander)
* mgr: Ignore __pycache__ and wheelhouse dirs (`pr#26481 <https://github.com/ceph/ceph/pull/26481>`_, Volker Theile)
* mgr: Improve ActivePyModules::get_typed_config implementation (`pr#26149 <https://github.com/ceph/ceph/pull/26149>`_, Volker Theile)
* mgr: improve docs for MgrModule methods (`pr#22792 <https://github.com/ceph/ceph/pull/22792>`_, John Spray)
* mgr: improvements for dynamic osd perf counters (`pr#25488 <https://github.com/ceph/ceph/pull/25488>`_, Mykola Golub)
* mgr: Include daemon details in SLOW_OPS output (`issue#23205 <http://tracker.ceph.com/issues/23205>`_, `pr#21750 <https://github.com/ceph/ceph/pull/21750>`_, Brad Hubbard)
* mgr: `#include <vector>` for clang (`pr#22756 <https://github.com/ceph/ceph/pull/22756>`_, Willem Jan Withagen)
* mgr: keep status, balancer always on (`pr#23558 <https://github.com/ceph/ceph/pull/23558>`_, Sage Weil)
* mgr: make module error message more descriptive (`pr#25537 <https://github.com/ceph/ceph/pull/25537>`_, Joao Eduardo Luis)
* mgr: mgr/ansible: Ansible orchestrator module (`pr#24445 <https://github.com/ceph/ceph/pull/24445>`_, Juan Miguel Olmo Martínez)
* mgr: mgr/ansible: Create/Remove OSDs (`pr#25497 <https://github.com/ceph/ceph/pull/25497>`_, Juan Miguel Olmo Martínez)
* mgr: mgr/ansible: Python 3 fix (`pr#25645 <https://github.com/ceph/ceph/pull/25645>`_, Sebastian Wagner)
* mgr: mgr/balancer: add min/max fields for weekday and be compatible with C (`pr#26505 <https://github.com/ceph/ceph/pull/26505>`_, xie xingguo)
* mgr: mgr/balancer: auto balance a list of pools (`pr#25940 <https://github.com/ceph/ceph/pull/25940>`_, xie xingguo)
* mgr: mgr/balancer: blame if upmap won't actually work (`pr#25941 <https://github.com/ceph/ceph/pull/25941>`_, xie xingguo)
* mgr: mgr/balancer: deepcopy best plan - otherwise we get latest (`issue#27000 <http://tracker.ceph.com/issues/27000>`_, `pr#23682 <https://github.com/ceph/ceph/pull/23682>`_, Stefan Priebe)
* mgr: mgr/balancer: restrict automatic balancing to specific weekdays (`pr#26440 <https://github.com/ceph/ceph/pull/26440>`_, xie xingguo)
* mgr: mgr/balancer: skip auto-balancing for pools with pending pg-merge (`pr#25626 <https://github.com/ceph/ceph/pull/25626>`_, xie xingguo)
* mgr: mgrc: enable disabling stats via mgr_stats_threshold (`issue#25197 <http://tracker.ceph.com/issues/25197>`_, `pr#23352 <https://github.com/ceph/ceph/pull/23352>`_, John Spray)
* mgr: mgr/crash: add hour granularity crash summary (`pr#23121 <https://github.com/ceph/ceph/pull/23121>`_, Noah Watkins)
* mgr: mgr/crash: add process name to crash metadata (`pr#25244 <https://github.com/ceph/ceph/pull/25244>`_, Mykola Golub)
* mgr: mgr/crash: fix python3 invalid syntax problems (`pr#23800 <https://github.com/ceph/ceph/pull/23800>`_, Ricardo Dias)
* mgr: mgr/DaemonServer: add js-output for "ceph osd safe-to-destroy" (`pr#24799 <https://github.com/ceph/ceph/pull/24799>`_, xie xingguo)
* mgr: mgr/DaemonServer: log pgmap usage to cluster log (`pr#26105 <https://github.com/ceph/ceph/pull/26105>`_, Neha Ojha)
* mgr: mgr/dashboard: Add option to disable SSL (`pr#22593 <https://github.com/ceph/ceph/pull/22593>`_, Wido den Hollander)
* mgr: mgr/dashboard: disable backend tests coverage (`pr#24193 <https://github.com/ceph/ceph/pull/24193>`_, Alfonso Martínez)
* mgr: mgr/dashboard: Fix dashboard shutdown/restart (`pr#22159 <https://github.com/ceph/ceph/pull/22159>`_, Boris Ranto)
* mgr: mgr/dashboard: Listen on port 8443 by default and not 8080 (`pr#22409 <https://github.com/ceph/ceph/pull/22409>`_, Wido den Hollander)
* mgr: mgr/dashboard: use the orchestrator_cli backend setting (`pr#26325 <https://github.com/ceph/ceph/pull/26325>`_, Jeff Layton)
* mgr: mgr/deepsea: always use 'password' parameter for salt-api auth (`pr#26904 <https://github.com/ceph/ceph/pull/26904>`_, Tim Serong)
* mgr: mgr/deepsea: check for inflight completions when starting event reader, cleanup logging and comments (`pr#25391 <https://github.com/ceph/ceph/pull/25391>`_, Tim Serong)
* mgr: mgr/deepsea: DeepSea orchestrator module (`pr#24610 <https://github.com/ceph/ceph/pull/24610>`_, Tim Serong)
* mgr: mgr/devicehealth: clean up error handling (`pr#23205 <https://github.com/ceph/ceph/pull/23205>`_, John Spray)
* mgr: mgr/devicehealth: fix is_valid_daemon_name typo error (`pr#24822 <https://github.com/ceph/ceph/pull/24822>`_, Lan Liu)
* mgr: mgr/diskprediction_cloud: fix divide by zero when total_size is 0 (`pr#26045 <https://github.com/ceph/ceph/pull/26045>`_, Rick Chen)
* mgr: mgr/diskprediction_cloud: Remove needless library in the requirements file (`issue#37533 <http://tracker.ceph.com/issues/37533>`_, `pr#25433 <https://github.com/ceph/ceph/pull/25433>`_, Rick Chen)
* mgr: mgr/influx: Use Queue to store points which need to be written (`pr#23464 <https://github.com/ceph/ceph/pull/23464>`_, Wido den Hollander)
* mgr: mgr/insights: insights reporting module (`pr#23497 <https://github.com/ceph/ceph/pull/23497>`_, Noah Watkins)
* mgr: mgr/mgr_module.py: fix doc for set_store/set_store_json (`pr#22654 <https://github.com/ceph/ceph/pull/22654>`_, Dan Mick)
* mgr: mgr/orchestrator: Add RGW service support (`pr#23702 <https://github.com/ceph/ceph/pull/23702>`_, Rubab-Syed)
* mgr: mgr/orchestrator: Add service_action method (`pr#25649 <https://github.com/ceph/ceph/pull/25649>`_, Tim Serong)
* mgr: mgr/orchestrator: Add support for "ceph orchestrator service ls" (`pr#24863 <https://github.com/ceph/ceph/pull/24863>`_, Jeff Layton)
* mgr: mgr/orchestrator: Improve debuggability (`pr#24147 <https://github.com/ceph/ceph/pull/24147>`_, Sebastian Wagner)
* mgr: mgr/orchestrator: Improve docstrings, add type hinting (`pr#25669 <https://github.com/ceph/ceph/pull/25669>`_, Sebastian Wagner)
* mgr: mgr/orchestrator: Simplify Orchestrator wait implementation (`pr#25401 <https://github.com/ceph/ceph/pull/25401>`_, Juan Miguel Olmo Martínez)
* mgr: mgr/orchestrator: use result property in Completion classes (`pr#24672 <https://github.com/ceph/ceph/pull/24672>`_, Tim Serong)
* mgr: mgr/progress: improve+test OSD out handling (`pr#23146 <https://github.com/ceph/ceph/pull/23146>`_, John Spray)
* mgr: mgr/progress: introduce the `progress` module (`pr#22993 <https://github.com/ceph/ceph/pull/22993>`_, John Spray)
* mgr: mgr/prometheus: Add recovery metrics (`pr#26880 <https://github.com/ceph/ceph/pull/26880>`_, Paul Cuzner)
* mgr: mgr/prometheus: get osd_objectstore once instead twice (`pr#26558 <https://github.com/ceph/ceph/pull/26558>`_, Konstantin Shalygin)
* mgr: mgr/restful: Fix deep-scrub typo (`issue#36720 <http://tracker.ceph.com/issues/36720>`_, `pr#24841 <https://github.com/ceph/ceph/pull/24841>`_, Boris Ranto)
* mgr: mgr/restful: fix py got exception when get osd info (`pr#21138 <https://github.com/ceph/ceph/pull/21138>`_, zouaiguo)
* mgr: mgr/restful: updated string formatting to str.format() (`pr#26210 <https://github.com/ceph/ceph/pull/26210>`_, James McClune)
* mgr: mgr/rook: fix API version and object types for recent rook changes (`pr#25452 <https://github.com/ceph/ceph/pull/25452>`_, Jeff Layton)
* mgr: mgr/rook: Fix Rook cluster name detection (`pr#24560 <https://github.com/ceph/ceph/pull/24560>`_, Sebastian Wagner)
* mgr: mgr/rook: update for v1beta1 API (`pr#23570 <https://github.com/ceph/ceph/pull/23570>`_, John Spray)
* mgr: mgr/status: Add standby-replay MDS ceph version (`pr#23624 <https://github.com/ceph/ceph/pull/23624>`_, Zhi Zhang)
* mgr: mgr/status: output to stdout, not stderr (`issue#24175 <http://tracker.ceph.com/issues/24175>`_, `pr#22089 <https://github.com/ceph/ceph/pull/22089>`_, John Spray)
* mgr: mgr/telegraf: Send more PG status information to Telegraf (`pr#22436 <https://github.com/ceph/ceph/pull/22436>`_, Wido den Hollander)
* mgr: mgr/telegraf: Telegraf module for Ceph Mgr (`pr#21782 <https://github.com/ceph/ceph/pull/21782>`_, Wido den Hollander)
* mgr: mgr/telegraf: Use Python generator and catch OSError (`pr#22418 <https://github.com/ceph/ceph/pull/22418>`_, Wido den Hollander)
* mgr: mgr/telemetry: Add Ceph Telemetry module to send reports back to project (`pr#21982 <https://github.com/ceph/ceph/pull/21982>`_, Wido den Hollander)
* mgr: mgr/telemetry: Check if boolean is False or not present (`pr#22223 <https://github.com/ceph/ceph/pull/22223>`_, Wido den Hollander)
* mgr: mgr/telemetry: Fix various issues (`pr#25770 <https://github.com/ceph/ceph/pull/25770>`_, Volker Theile)
* mgr: mgr/volumes: fix orchestrator remove operation (`pr#25339 <https://github.com/ceph/ceph/pull/25339>`_, Jeff Layton)
* mgr: mgr/zabbix: drop "total_objects" field (`pr#26052 <https://github.com/ceph/ceph/pull/26052>`_, Kefu Chai)
* mgr: mgr/zabbix: Send more PG information to Zabbix (`pr#22434 <https://github.com/ceph/ceph/pull/22434>`_, Wido den Hollander)
* mgr: Miscellaneous small mgr fixes (`pr#22893 <https://github.com/ceph/ceph/pull/22893>`_, John Spray)
* mgr: modules CLI commands declaration using @CLICommand decorator (`pr#25543 <https://github.com/ceph/ceph/pull/25543>`_, Ricardo Dias)
* mgr,mon: mgr,mon: fix to apply changed mon_stat_smooth_intervals (`pr#23481 <https://github.com/ceph/ceph/pull/23481>`_, Yan Jun)
* mgr/orchestrator: added useful attributes to ServiceDescription (`pr#25468 <https://github.com/ceph/ceph/pull/25468>`_, Ricardo Dias)
* mgr/orchestrator: Add host mon mgr management to interface (`pr#26314 <https://github.com/ceph/ceph/pull/26314>`_, Sebastian Wagner, Noah Watkins)
* mgr/orchestrator: Add JSON output to CLI commands (`pr#25340 <https://github.com/ceph/ceph/pull/25340>`_, Sebastian Wagner)
* mgr: orchestrator: add the ability to remove services (`pr#25366 <https://github.com/ceph/ceph/pull/25366>`_, Jeff Layton)
* mgr/orchestrator: Allow the orchestrator to scale the NFS server count (`pr#26633 <https://github.com/ceph/ceph/pull/26633>`_, Jeff Layton)
* mgr/orchestrator: clarify error message about kubernetes python module (`pr#24525 <https://github.com/ceph/ceph/pull/24525>`_, Jeff Layton)
* mgr/orchestrator_cli: Fix README.md (`pr#26443 <https://github.com/ceph/ceph/pull/26443>`_, Sebastian Wagner)
* mgr/orchestrator: Extend DriveGroupSpec (`pr#25912 <https://github.com/ceph/ceph/pull/25912>`_, Sebastian Wagner)
* mgr/orchestrator: fix device pretty print with None attributes (`pr#26357 <https://github.com/ceph/ceph/pull/26357>`_, Ricardo Dias)
* mgr/orchestrator: fix _list_services display (`pr#25610 <https://github.com/ceph/ceph/pull/25610>`_, Jeff Layton)
* mgr/orchestrator: Fix up rook osd create dispatcher (`pr#26317 <https://github.com/ceph/ceph/pull/26317>`_, Jeff Layton)
* mgr/orchestrator: make use of @CLICommand (`pr#26094 <https://github.com/ceph/ceph/pull/26094>`_, Sebastian Wagner)
* mgr/orchestrator: remove unicode whitespaces (`pr#25323 <https://github.com/ceph/ceph/pull/25323>`_, Sebastian Wagner)
* mgr/orchestrator/rook: allow the creation of OSDs in directories (`pr#26570 <https://github.com/ceph/ceph/pull/26570>`_, Jeff Layton)
* mgr/orchestrator: Unify `osd create` and `osd add` (`pr#26171 <https://github.com/ceph/ceph/pull/26171>`_, Sebastian Wagner)
* mgr/orch: refresh option for inventory query (`pr#26346 <https://github.com/ceph/ceph/pull/26346>`_, Noah Watkins)
* mgr: prometheus: added bluestore db and wal/journal devices to ceph_disk_occupation metric (`issue#36627 <http://tracker.ceph.com/issues/36627>`_, `pr#24821 <https://github.com/ceph/ceph/pull/24821>`_, Konstantin Shalygin)
* mgr: prometheus: Expose number of degraded/misplaced/unfound objects (`pr#21793 <https://github.com/ceph/ceph/pull/21793>`_, Boris Ranto)
* mgr: prometheus: Fix metric resets (`pr#22732 <https://github.com/ceph/ceph/pull/22732>`_, Boris Ranto)
* mgr: prometheus: Fix prometheus shutdown/restart (`pr#21748 <https://github.com/ceph/ceph/pull/21748>`_, Boris Ranto)
* mgr: pybind/mgr: add osd space utilization to insights report (`pr#25122 <https://github.com/ceph/ceph/pull/25122>`_, Noah Watkins)
* mgr: pybind/mgr: PEP 8 code clean and fix typo (`pr#26181 <https://github.com/ceph/ceph/pull/26181>`_, Lei Liu)
* mgr,pybind: mgr/prometheus: add interface and objectstore to osd metadata (`pr#25234 <https://github.com/ceph/ceph/pull/25234>`_, Jan Fajerski)
* mgr: pybind/mgr/restful: Decode the output of b64decode (`issue#38522 <http://tracker.ceph.com/issues/38522>`_, `pr#26712 <https://github.com/ceph/ceph/pull/26712>`_, Brad Hubbard)
* mgr,pybind: mgr/rook: fix urljoin import (`pr#24626 <https://github.com/ceph/ceph/pull/24626>`_, Jeff Layton)
* mgr,pybind: mgr/volumes: Fix Python 3 import error (`pr#25344 <https://github.com/ceph/ceph/pull/25344>`_, Sebastian Wagner)
* mgr,pybind: pybind/mgr: drop unnecessary iterkeys usage to make py-3 compatible (`issue#37581 <http://tracker.ceph.com/issues/37581>`_, `pr#25457 <https://github.com/ceph/ceph/pull/25457>`_, Mykola Golub)
* mgr,pybind: pybind/mgr: identify invalid fs (`pr#24392 <https://github.com/ceph/ceph/pull/24392>`_, Jos Collin)
* mgr,pybind: src/script: add run_mypy to run static type checking on Python code (`pr#26715 <https://github.com/ceph/ceph/pull/26715>`_, Sebastian Wagner)
* mgr: race between daemon state and service map in 'service status' (`issue#36656 <http://tracker.ceph.com/issues/36656>`_, `pr#24878 <https://github.com/ceph/ceph/pull/24878>`_, Mykola Golub)
* mgr,rbd: mgr/prometheus: provide RBD stats via osd dynamic perf counters (`pr#25358 <https://github.com/ceph/ceph/pull/25358>`_, Mykola Golub)
* mgr,rbd: pybind/mgr/prometheus: improve 'rbd_stats_pools' param parsing (`pr#25860 <https://github.com/ceph/ceph/pull/25860>`_, Mykola Golub)
* mgr,rbd: pybind/mgr/prometheus: rbd stats namespace support (`pr#25636 <https://github.com/ceph/ceph/pull/25636>`_, Mykola Golub)
* mgr: replace "Unknown error" string on always_on (`pr#23645 <https://github.com/ceph/ceph/pull/23645>`_, John Spray)
* mgr: restful: Fix regression when traversing leaf nodes (`pr#26421 <https://github.com/ceph/ceph/pull/26421>`_, Boris Ranto)
* mgr/rook: remove dead code and fix bug in url fetching code (`pr#26032 <https://github.com/ceph/ceph/pull/26032>`_, Jeff Layton)
* mgr: silence GCC warning (`pr#25199 <https://github.com/ceph/ceph/pull/25199>`_, Kefu Chai)
* mgr/ssh: fix type and doc errors (`pr#26630 <https://github.com/ceph/ceph/pull/26630>`_, Sebastian Wagner)
* mgr/telemetry: fix total_objects (`issue#37976 <http://tracker.ceph.com/issues/37976>`_, `pr#26046 <https://github.com/ceph/ceph/pull/26046>`_, Sage Weil)
* mgr,tests: mgr/dashboard: use dedicated tox working dir (`pr#25290 <https://github.com/ceph/ceph/pull/25290>`_, Noah Watkins)
* mgr,tests: mgr/insights: use dedicated tox working dir (`pr#25146 <https://github.com/ceph/ceph/pull/25146>`_, Noah Watkins)
* mgr,tests: mgr/selftest: fix disabled module selection (`pr#24517 <https://github.com/ceph/ceph/pull/24517>`_, John Spray)
* mgr: timely health updates between monitor and manager (`pr#23294 <https://github.com/ceph/ceph/pull/23294>`_, Noah Watkins)
* mgr: update daemon_state when necessary (`issue#37753 <http://tracker.ceph.com/issues/37753>`_, `pr#25725 <https://github.com/ceph/ceph/pull/25725>`_, Xinying Song)
* mgr: update MMgrConfigure message to include optional OSD perf queries (`pr#24180 <https://github.com/ceph/ceph/pull/24180>`_, Julien Collet)
* mgr: Use Py_BuildValue to create the argument tuple (`pr#26240 <https://github.com/ceph/ceph/pull/26240>`_, Volker Theile)
* mgr: volumes mgr module fixes (`pr#25331 <https://github.com/ceph/ceph/pull/25331>`_, Jeff Layton)
* misc: mark functions with 'override' specifier (`pr#21790 <https://github.com/ceph/ceph/pull/21790>`_, Danny Al-Gaaf)
* mon: add 'osd destroy-new' command that only destroys NEW osd slots (`issue#24428 <http://tracker.ceph.com/issues/24428>`_, `pr#22429 <https://github.com/ceph/ceph/pull/22429>`_, Sage Weil)
* mon: A PG with PG_STATE_REPAIR doesn't mean damaged data, PG_STATE_IN… (`issue#38070 <http://tracker.ceph.com/issues/38070>`_, `pr#26178 <https://github.com/ceph/ceph/pull/26178>`_, David Zafman)
* mon: change monitor compact command to run asynchronously (`issue#24160 <http://tracker.ceph.com/issues/24160>`_, `issue#24159 <http://tracker.ceph.com/issues/24159>`_, `pr#22056 <https://github.com/ceph/ceph/pull/22056>`_, penglaiyxy)
* mon: common/cmdparse: cmd_getval_throws -> cmd_getval (`pr#23557 <https://github.com/ceph/ceph/pull/23557>`_, Sage Weil)
* mon: don't commit osdmap on no-op application ops (`pr#23528 <https://github.com/ceph/ceph/pull/23528>`_, John Spray)
* mon: fix mgr module config option handling (`issue#35076 <http://tracker.ceph.com/issues/35076>`_, `pr#23846 <https://github.com/ceph/ceph/pull/23846>`_, Sage Weil)
* mon: fix pg_sum_old not copied correctly (`pr#26110 <https://github.com/ceph/ceph/pull/26110>`_, Yao Zongyou)
* monitoring/grafana: Fix OSD Capacity Utlization Grafana graph (`pr#24426 <https://github.com/ceph/ceph/pull/24426>`_, Maxime)
* mon: make rank ordering explicit (not tied to mon address sort order) (`pr#22193 <https://github.com/ceph/ceph/pull/22193>`_, Sage Weil)
* mon: mon/config-key: increase max key entry size (`pr#24250 <https://github.com/ceph/ceph/pull/24250>`_, Joao Eduardo Luis)
* mon: mon/MonClient: drop my_addr (`pr#26449 <https://github.com/ceph/ceph/pull/26449>`_, Kefu Chai)
* mon: mon/MonClient: use mon_client_ping_timeout during ping_monitor (`pr#23563 <https://github.com/ceph/ceph/pull/23563>`_, Yao Zongyou)
* mon: mon/MonMap: add more const'ness to its methods (`pr#23709 <https://github.com/ceph/ceph/pull/23709>`_, Kefu Chai)
* mon: mon/MonMap: remove duplicate code in get_rank (`pr#23547 <https://github.com/ceph/ceph/pull/23547>`_, Yao Zongyou)
* mon: mon,osd: avoid str copy in parse (`pr#25640 <https://github.com/ceph/ceph/pull/25640>`_, Jos Collin)
* mon: mon/OSDMonitor: add boundary check for pool recovery_priority (`issue#38578 <http://tracker.ceph.com/issues/38578>`_, `pr#26729 <https://github.com/ceph/ceph/pull/26729>`_, xie xingguo)
* mon: mon/PGMap: add more #include (`pr#26420 <https://github.com/ceph/ceph/pull/26420>`_, Kefu Chai)
* mon: mon/PGMap: command 'ceph df -f json' output add total_percent_used (`pr#23588 <https://github.com/ceph/ceph/pull/23588>`_, Yanhu Cao)
* mon: only share monmap after authenticating (`pr#23741 <https://github.com/ceph/ceph/pull/23741>`_, Sage Weil)
* mon: shutdown messenger early to avoid accessing deleted logger (`issue#37780 <http://tracker.ceph.com/issues/37780>`_, `pr#25760 <https://github.com/ceph/ceph/pull/25760>`_, ningtao)
* mon: some tiny cleanups related class forward declaration (`pr#26219 <https://github.com/ceph/ceph/pull/26219>`_, Yao Zongyou)
* mon,tests: qa/cephtool: test bounds on pool's `hit_set_\*` (`pr#24858 <https://github.com/ceph/ceph/pull/24858>`_, Joao Eduardo Luis)
* mon:validate hit_set values before set (`issue#22659 <http://tracker.ceph.com/issues/22659>`_, `pr#19983 <https://github.com/ceph/ceph/pull/19983>`_, lijing)
* msg: addr -> addrvec (part 1) (`pr#22306 <https://github.com/ceph/ceph/pull/22306>`_, Sage Weil)
* msg/async: do not force updating rotating keys inline (`pr#25859 <https://github.com/ceph/ceph/pull/25859>`_, yanjun, xie xingguo)
* msg/async/Protocol\*: send keep alive if existing wins (`issue#38493 <http://tracker.ceph.com/issues/38493>`_, `pr#26668 <https://github.com/ceph/ceph/pull/26668>`_, xie xingguo)
* msg/async/rdma: add iWARP RDMA protocol support (`pr#20297 <https://github.com/ceph/ceph/pull/20297>`_, Haodong Tang)
* msg/async/rdma: Delete duplicate header file (`pr#25392 <https://github.com/ceph/ceph/pull/25392>`_, Jianpeng Ma)
* msg/async/rdma: parse IBSYNMsg.lid as hex when receiving message (`pr#26525 <https://github.com/ceph/ceph/pull/26525>`_, Peng Liu)
* msg/async: reduce additional ceph_msg_header copy (`pr#25938 <https://github.com/ceph/ceph/pull/25938>`_, Jianpeng Ma)
* msg/async: the ceph_abort is needless in handle_connect_msg (`pr#21751 <https://github.com/ceph/ceph/pull/21751>`_, shangfufei)
* msg: ceph_abort() when there are enough accepter errors in msg server (`issue#23649 <http://tracker.ceph.com/issues/23649>`_, `pr#23306 <https://github.com/ceph/ceph/pull/23306>`_, penglaiyxy@gmail.com)
* msg: clear message middle when clearing encoded message buffer (`pr#24289 <https://github.com/ceph/ceph/pull/24289>`_, "Yan, Zheng")
* msg: entity_addr_t::parse doesn't do memset(this, 0, ...) for clean-up (`issue#26937 <http://tracker.ceph.com/issues/26937>`_, `pr#23573 <https://github.com/ceph/ceph/pull/23573>`_, Radoslaw Zarzynski)
* nautilus: mgr/dashboard: Validate `ceph-iscsi` config version (`pr#26951 <https://github.com/ceph/ceph/pull/26951>`_, Ricardo Marques)
* objecter: avoid race when reset down osd's session (`pr#25437 <https://github.com/ceph/ceph/pull/25437>`_, Zengran Zhang)
* orchestrator_cli: fix HandleCommandResult invocations in _status() (`pr#25329 <https://github.com/ceph/ceph/pull/25329>`_, Jeff Layton)
* osd: add creating to pg_string_state (`issue#36174 <http://tracker.ceph.com/issues/36174>`_, `pr#24262 <https://github.com/ceph/ceph/pull/24262>`_, Dan van der Ster)
* osd: add --dump-journal option in ceph-osd help info (`pr#24969 <https://github.com/ceph/ceph/pull/24969>`_, yuliyang)
* osd: Additional fields for osd "bench" command (`pr#21962 <https://github.com/ceph/ceph/pull/21962>`_, Коренберг Маркr)
* osd: add log when pg reg next scrub (`pr#23690 <https://github.com/ceph/ceph/pull/23690>`_, lvshuhua)
* osd: add required cls libraries as dependencies of osd (`pr#24373 <https://github.com/ceph/ceph/pull/24373>`_, Mohamad Gebai)
* osd: Allow repair of an object with a bad data_digest in object_info on all replicas (`pr#23217 <https://github.com/ceph/ceph/pull/23217>`_, David Zafman)
* osd: always set query_epoch explicitly for MOSDPGLog (`pr#22487 <https://github.com/ceph/ceph/pull/22487>`_, Kefu Chai)
* osd: avoid using null agent_state (`pr#25393 <https://github.com/ceph/ceph/pull/25393>`_, Zengran Zhang)
* osd: Change assert() to ceph_assert() missed in the transition (`pr#23918 <https://github.com/ceph/ceph/pull/23918>`_, David Zafman)
* osd: Change osd_skip_data_digest default to false and make it LEVEL_DEV (`issue#24950 <http://tracker.ceph.com/issues/24950>`_, `pr#23083 <https://github.com/ceph/ceph/pull/23083>`_, Sage Weil, David Zafman)
* osdc: invoke notify finish context on linger commit failure (`issue#23966 <http://tracker.ceph.com/issues/23966>`_, `pr#21831 <https://github.com/ceph/ceph/pull/21831>`_, Kefu Chai, Jason Dillaman)
* osd: clean up and avoid extra ref-counting in PrimaryLogPG::log_op_stats (`pr#23016 <https://github.com/ceph/ceph/pull/23016>`_, Radoslaw Zarzynski)
* osd: clean up smart probe (`issue#23899 <http://tracker.ceph.com/issues/23899>`_, `pr#21950 <https://github.com/ceph/ceph/pull/21950>`_, Sage Weil, Gu Zhongyan)
* osd: collect client perf stats when query is enabled (`pr#24265 <https://github.com/ceph/ceph/pull/24265>`_, Julien Collet, Mykola Golub)
* osd: combine recovery/scrub/snap sleep timer into one (`pr#21711 <https://github.com/ceph/ceph/pull/21711>`_, Jianpeng Ma)
* osd: Deny reservation if expected backfill size would put us over bac… (`issue#24801 <http://tracker.ceph.com/issues/24801>`_, `issue#19753 <http://tracker.ceph.com/issues/19753>`_, `pr#22797 <https://github.com/ceph/ceph/pull/22797>`_, David Zafman)
* osd: do not include Messenger.h if not necessary (`pr#22483 <https://github.com/ceph/ceph/pull/22483>`_, Kefu Chai)
* osd: do not overestimate the size of the object for reads with trimtrunc (`issue#21931 <http://tracker.ceph.com/issues/21931>`_, `issue#22330 <http://tracker.ceph.com/issues/22330>`_, `pr#24564 <https://github.com/ceph/ceph/pull/24564>`_, Neha Ojha)
* osd: do not treat an IO hint as an IOP for PG stats (`issue#24909 <http://tracker.ceph.com/issues/24909>`_, `pr#23029 <https://github.com/ceph/ceph/pull/23029>`_, Jason Dillaman)
* osd: don't check overwrite flag when handling copy-get (`issue#21756 <http://tracker.ceph.com/issues/21756>`_, `pr#18241 <https://github.com/ceph/ceph/pull/18241>`_, huangjun)
* osd: Don't evict even when preemption has restarted with smaller chunk (`pr#21892 <https://github.com/ceph/ceph/pull/21892>`_, David Zafman)
* osd: do_sparse_read(): Verify checksum earlier so we will try to repair (`issue#24875 <http://tracker.ceph.com/issues/24875>`_, `pr#23377 <https://github.com/ceph/ceph/pull/23377>`_, David Zafman)
* osd: drop the unused request_redirect_t::osd_instructions (`pr#24458 <https://github.com/ceph/ceph/pull/24458>`_, Radoslaw Zarzynski)
* osd: ec saves a write access to the memory under most circumstances (`pr#26053 <https://github.com/ceph/ceph/pull/26053>`_, Zengran Zhang, Kefu Chai)
* osd: fix build_incremental_map_msg (`issue#38282 <http://tracker.ceph.com/issues/38282>`_, `pr#26413 <https://github.com/ceph/ceph/pull/26413>`_, Sage Weil)
* osd: fix memory leak in EC fast and error read (`pr#22500 <https://github.com/ceph/ceph/pull/22500>`_, xiaofei cui)
* osd: Fix recovery and backfill priority handling (`issue#38041 <http://tracker.ceph.com/issues/38041>`_, `pr#26213 <https://github.com/ceph/ceph/pull/26213>`_, David Zafman)
* osd: fix shard_info_wrapper encode (`issue#37653 <http://tracker.ceph.com/issues/37653>`_, `pr#25548 <https://github.com/ceph/ceph/pull/25548>`_, David Zafman)
* osd: Handle omap and data digests independently (`issue#24366 <http://tracker.ceph.com/issues/24366>`_, `pr#22346 <https://github.com/ceph/ceph/pull/22346>`_, David Zafman)
* osd: increase default hard pg limit (`pr#22187 <https://github.com/ceph/ceph/pull/22187>`_, Josh Durgin)
* osd: keep using cache even if op will invalid cache (`pr#25490 <https://github.com/ceph/ceph/pull/25490>`_, Zengran Zhang)
* osd: limit pg log length under all circumstances (`pr#23098 <https://github.com/ceph/ceph/pull/23098>`_, Neha Ojha)
* osd: make OSD::HEARTBEAT_MAX_CONN inline (`pr#23424 <https://github.com/ceph/ceph/pull/23424>`_, Kefu Chai)
* osd: make random shuffle comply with C++17 (`pr#23533 <https://github.com/ceph/ceph/pull/23533>`_, Willem Jan Withagen)
* osd/OSDMap: add osd status to utilization dumper (`issue#35544 <http://tracker.ceph.com/issues/35544>`_, `pr#23921 <https://github.com/ceph/ceph/pull/23921>`_, Paul Emmerich)
* osd: per-pool osd stats collection (`pr#19454 <https://github.com/ceph/ceph/pull/19454>`_, Igor Fedotv, Igor Fedotov)
* osd: Prevent negative local num_bytes sent to peer for backfill reser… (`issue#38344 <http://tracker.ceph.com/issues/38344>`_, `pr#26465 <https://github.com/ceph/ceph/pull/26465>`_, David Zafman)
* osd: read object attrs failed at EC recovery (`pr#22196 <https://github.com/ceph/ceph/pull/22196>`_, xiaofei cui)
* osd: refuse to start if we're > N+2 from recorded require_osd_release (`issue#38076 <http://tracker.ceph.com/issues/38076>`_, `pr#26177 <https://github.com/ceph/ceph/pull/26177>`_, Sage Weil)
* osd: reliably send pg_created messages to the mon (`issue#37775 <http://tracker.ceph.com/issues/37775>`_, `pr#25731 <https://github.com/ceph/ceph/pull/25731>`_, Sage Weil)
* osd: Remove old bft= which has been superceded by backfill (`issue#36170 <http://tracker.ceph.com/issues/36170>`_, `pr#24256 <https://github.com/ceph/ceph/pull/24256>`_, David Zafman)
* osd: remove stray derr (`pr#24042 <https://github.com/ceph/ceph/pull/24042>`_, Sage Weil)
* osd: remove unused class read_log_and_missing_error (`pr#26057 <https://github.com/ceph/ceph/pull/26057>`_, Yao Zongyou)
* osd: remove unused fields (`pr#26021 <https://github.com/ceph/ceph/pull/26021>`_, Jianpeng Ma)
* osd: remove unused function (`pr#26223 <https://github.com/ceph/ceph/pull/26223>`_, Jianpeng Ma)
* osd: Remove useless conditon (`pr#21766 <https://github.com/ceph/ceph/pull/21766>`_, Jianpeng Ma)
* osd: some recovery improvements and cleanups (`pr#23663 <https://github.com/ceph/ceph/pull/23663>`_, xie xingguo)
* osd: two heartbeat fixes (`pr#25126 <https://github.com/ceph/ceph/pull/25126>`_, xie xingguo)
* osd: unlock osd_lock when tweaking osd settings (`issue#37751 <http://tracker.ceph.com/issues/37751>`_, `pr#25726 <https://github.com/ceph/ceph/pull/25726>`_, Kefu Chai)
* osd: unmount store after service.shutdown() (`issue#37975 <http://tracker.ceph.com/issues/37975>`_, `pr#26043 <https://github.com/ceph/ceph/pull/26043>`_, Kefu Chai)
* osd: Weighted Random Sampling for dynamic perf stats (`pr#25582 <https://github.com/ceph/ceph/pull/25582>`_, Mykola Golub)
* osd: When possible check CRC in build_push_op() so repair can eventually stop (`issue#25084 <http://tracker.ceph.com/issues/25084>`_, `pr#23518 <https://github.com/ceph/ceph/pull/23518>`_, David Zafman)
* osd: write "bench" output to stdout (`issue#24022 <http://tracker.ceph.com/issues/24022>`_, `pr#21905 <https://github.com/ceph/ceph/pull/21905>`_, John Spray)
* os: Minor fixes in comments describing a transaction (`pr#22329 <https://github.com/ceph/ceph/pull/22329>`_, Bryan Stillwell)
* performance: Add performance counters breadcrumb (`pr#22060 <https://github.com/ceph/ceph/pull/22060>`_, Ricardo Marques)
* performance: mgr/dashboard: Enable gzip compression (`issue#36453 <http://tracker.ceph.com/issues/36453>`_, `pr#24727 <https://github.com/ceph/ceph/pull/24727>`_, Zack Cerza)
* performance: mgr/dashboard: Replace dashboard service (`issue#36675 <http://tracker.ceph.com/issues/36675>`_, `pr#24900 <https://github.com/ceph/ceph/pull/24900>`_, Zack Cerza)
* performance: msg/async: improve read-prefetch logic (`pr#25758 <https://github.com/ceph/ceph/pull/25758>`_, xie xingguo)
* performance: qa/tasks/cbt.py: changes to run on bionic (`pr#22405 <https://github.com/ceph/ceph/pull/22405>`_, Neha Ojha)
* performance,rbd: common/Throttle: TokenBucketThrottle: use reference to m_blockers.front() (`issue#36475 <http://tracker.ceph.com/issues/36475>`_, `pr#24604 <https://github.com/ceph/ceph/pull/24604>`_, Dongsheng Yang)
* performance,rbd: pybind/rbd: optimize rbd_list2 (`pr#25445 <https://github.com/ceph/ceph/pull/25445>`_, Mykola Golub)
* Prevent duplicated rows during async tasks (`pr#22148 <https://github.com/ceph/ceph/pull/22148>`_, Ricardo Marques)
* prometheus: Fix order of occupation values (`pr#22149 <https://github.com/ceph/ceph/pull/22149>`_, Boris Ranto)
* pybind: do not check MFLAGS (`pr#23601 <https://github.com/ceph/ceph/pull/23601>`_, Kefu Chai)
* pybind: pybind/ceph_daemon: expand the order of magnitude of daemonperf statistics to ZB (`issue#23962 <http://tracker.ceph.com/issues/23962>`_, `pr#21765 <https://github.com/ceph/ceph/pull/21765>`_, Guan yunfei)
* pybind: pybind/rbd: make the code more concise (`pr#23664 <https://github.com/ceph/ceph/pull/23664>`_, Zheng Yin)
* pybind,rbd: pybind/rbd: add allow_shrink=True as a parameter to def resize (`pr#23605 <https://github.com/ceph/ceph/pull/23605>`_, Zheng Yin)
* pybind,rbd: pybind/rbd: fix a typo in metadata_get comments (`pr#26138 <https://github.com/ceph/ceph/pull/26138>`_, songweibin)
* pybind,rgw: pybind/rgw: pass the flags to callback function (`pr#25766 <https://github.com/ceph/ceph/pull/25766>`_, Kefu Chai)
* pybind: simplify timeout handling in run_in_thread() (`pr#24733 <https://github.com/ceph/ceph/pull/24733>`_, Kefu Chai)
* qa/btrfs/test_rmdir_async_snap: remove binary file (`pr#24108 <https://github.com/ceph/ceph/pull/24108>`_, Cleber Rosa)
* qa,pybind,tools: Correct usage of collections.abc (`pr#25318 <https://github.com/ceph/ceph/pull/25318>`_, James Page)
* qa/test: Added rados, rbd and fs to run two time a week only (`pr#21839 <https://github.com/ceph/ceph/pull/21839>`_, Yuri Weinstein)
* qa/tests: added 1st draft of mimic-x suite (`pr#23292 <https://github.com/ceph/ceph/pull/23292>`_, Yuri Weinstein)
* qa/tests - added all supported distro (`pr#22647 <https://github.com/ceph/ceph/pull/22647>`_, Yuri Weinstein)
* qa/tests - added all supported distro to the mix, … (`pr#22674 <https://github.com/ceph/ceph/pull/22674>`_, Yuri Weinstein)
* qa/tests: added client-upgrade-luminous suit (`pr#21947 <https://github.com/ceph/ceph/pull/21947>`_, Yuri Weinstein)
* qa/tests: added --filter-out="ubuntu_14.04" (`pr#21949 <https://github.com/ceph/ceph/pull/21949>`_, Yuri Weinstein)
* qa/tests - added luminous-p2p suite to the schedule (`pr#22666 <https://github.com/ceph/ceph/pull/22666>`_, Yuri Weinstein)
* qa/tests: added mimic-x to the schedule (`pr#23302 <https://github.com/ceph/ceph/pull/23302>`_, Yuri Weinstein)
* qa/tests - added powercycle suite to run on weekly basis on master and mimic (`pr#22606 <https://github.com/ceph/ceph/pull/22606>`_, Yuri Weinstein)
* qa/tests:  added supported distro for powercycle suite (`pr#22185 <https://github.com/ceph/ceph/pull/22185>`_, Yuri Weinstein)
* qa/tests: changed ceph qa email address to bypass dreamhost's spam filter (`pr#23456 <https://github.com/ceph/ceph/pull/23456>`_, Yuri Weinstein)
* qa/tests: changed disto symlink to point to new way using supported OS'es (`pr#22536 <https://github.com/ceph/ceph/pull/22536>`_, Yuri Weinstein)
* qa/tests: fixed typo (`pr#21858 <https://github.com/ceph/ceph/pull/21858>`_, Yuri Weinstein)
* qa/tests: removed all jewel runs and reduced runs on ovh (`pr#22531 <https://github.com/ceph/ceph/pull/22531>`_, Yuri Weinstein)
* rbd: add 'config global' command to get/store overrides in mon config db (`pr#24428 <https://github.com/ceph/ceph/pull/24428>`_, Mykola Golub)
* rbd: add data pool support to trash purge (`issue#22872 <http://tracker.ceph.com/issues/22872>`_, `pr#21247 <https://github.com/ceph/ceph/pull/21247>`_, Mahati Chamarthy)
* rbd: add group snap rollback method (`issue#23550 <http://tracker.ceph.com/issues/23550>`_, `pr#23896 <https://github.com/ceph/ceph/pull/23896>`_, songweibin)
* rbd: add protected in snap list (`pr#23853 <https://github.com/ceph/ceph/pull/23853>`_, Zheng Yin)
* rbd: add snapshot count in rbd info (`pr#21292 <https://github.com/ceph/ceph/pull/21292>`_, Zheng Yin)
* rbd: add the judgment of resizing the image (`pr#21770 <https://github.com/ceph/ceph/pull/21770>`_, zhengyin)
* rbd: basic support for images within namespaces (`issue#24558 <http://tracker.ceph.com/issues/24558>`_, `pr#22673 <https://github.com/ceph/ceph/pull/22673>`_, Jason Dillaman)
* rbd: close image when bench is interrupted (`pr#26693 <https://github.com/ceph/ceph/pull/26693>`_, Mykola Golub)
* rbd: cls/lock: always store v1 addr in locker_info_t (`pr#25948 <https://github.com/ceph/ceph/pull/25948>`_, Sage Weil)
* rbd: cls/rbd: fix build (`pr#22078 <https://github.com/ceph/ceph/pull/22078>`_, Kefu Chai)
* rbd: cls/rbd: fixed uninitialized variable compiler warning (`pr#26896 <https://github.com/ceph/ceph/pull/26896>`_, Jason Dillaman)
* rbd: cls/rbd: fix method comment (`pr#23277 <https://github.com/ceph/ceph/pull/23277>`_, Zheng Yin)
* rbd: cls/rbd: silence the log of get metadata error (`pr#25436 <https://github.com/ceph/ceph/pull/25436>`_, songweibin)
* rbd: correct parameter of namespace and verify it before set_namespace (`pr#23770 <https://github.com/ceph/ceph/pull/23770>`_, songweibin)
* rbd: dashboard: support configuring block mirroring pools and peers (`pr#25210 <https://github.com/ceph/ceph/pull/25210>`_, Jason Dillaman)
* rbd: disable cache for actions that open multiple images (`issue#24092 <http://tracker.ceph.com/issues/24092>`_, `pr#21946 <https://github.com/ceph/ceph/pull/21946>`_, Jason Dillaman)
* rbd: disk-usage can now optionally compute exact on-disk usage (`issue#24064 <http://tracker.ceph.com/issues/24064>`_, `pr#21912 <https://github.com/ceph/ceph/pull/21912>`_, Jason Dillaman)
* rbd: Document new RBD feature flags and version support (`pr#25192 <https://github.com/ceph/ceph/pull/25192>`_, Valentin Lorentz)
* rbd: don't load config overrides from monitor initially (`pr#21910 <https://github.com/ceph/ceph/pull/21910>`_, Jason Dillaman)
* rbd: error if new size is equal to original size (`pr#22637 <https://github.com/ceph/ceph/pull/22637>`_, zhengyin)
* rbd: expose pool stats summary tool (`pr#24830 <https://github.com/ceph/ceph/pull/24830>`_, Jason Dillaman)
* rbd: filter out group/trash snapshots from snap_list (`pr#23638 <https://github.com/ceph/ceph/pull/23638>`_, songweibin)
* rbd: fix a typo in error output (`pr#25931 <https://github.com/ceph/ceph/pull/25931>`_, Dongsheng Yang)
* rbd: fix delay time calculation for trash move (`pr#25896 <https://github.com/ceph/ceph/pull/25896>`_, Mykola Golub)
* rbd: fix error import when the input is a pipe (`issue#34536 <http://tracker.ceph.com/issues/34536>`_, `pr#23835 <https://github.com/ceph/ceph/pull/23835>`_, songweibin)
* rbd: fix segmentation fault when rbd_group_image_list() getting -ENOENT (`issue#38468 <http://tracker.ceph.com/issues/38468>`_, `pr#26622 <https://github.com/ceph/ceph/pull/26622>`_, songweibin)
* rbd: fix some typos (`pr#25083 <https://github.com/ceph/ceph/pull/25083>`_, Shiyang Ruan)
* rbd: implement new 'rbd perf image iostat/iotop' commands (`issue#37913 <http://tracker.ceph.com/issues/37913>`_, `pr#26133 <https://github.com/ceph/ceph/pull/26133>`_, Jason Dillaman)
* rbd: improved trash snapshot namespace handling (`issue#23398 <http://tracker.ceph.com/issues/23398>`_, `pr#23191 <https://github.com/ceph/ceph/pull/23191>`_, Jason Dillaman)
* rbd: interlock object-map/fast-diff features together (`pr#21969 <https://github.com/ceph/ceph/pull/21969>`_, Mao Zhongyi)
* rbd: introduce abort_on_full option for rbd map (`pr#25662 <https://github.com/ceph/ceph/pull/25662>`_, Dongsheng Yang)
* rbd: journal: allow remove set when jounal pool is full (`pr#25166 <https://github.com/ceph/ceph/pull/25166>`_, kungf)
* rbd: journal: fix potential race when closing object recorder (`pr#26425 <https://github.com/ceph/ceph/pull/26425>`_, Mykola Golub)
* rbd:  journal: set max journal order to 26 (`issue#37541 <http://tracker.ceph.com/issues/37541>`_, `pr#25743 <https://github.com/ceph/ceph/pull/25743>`_, Mykola Golub)
* rbd: krbd: support for images within namespaces (`pr#23841 <https://github.com/ceph/ceph/pull/23841>`_, Ilya Dryomov)
* rbd: librbd/api: misc fix migration (`pr#25765 <https://github.com/ceph/ceph/pull/25765>`_, songweibin)
* rbd:  librbd: ensure exclusive lock acquired when removing sync point snapshots (`issue#24898 <http://tracker.ceph.com/issues/24898>`_, `pr#23095 <https://github.com/ceph/ceph/pull/23095>`_, Mykola Golub)
* rbd:  librbd: misc fix potential invalid pointer (`pr#25462 <https://github.com/ceph/ceph/pull/25462>`_, songweibin)
* rbd: make sure the return-value 'r' will be returned (`pr#24891 <https://github.com/ceph/ceph/pull/24891>`_, Shiyang Ruan)
* rbd: mgr/dashboard: incorporate RBD overall performance grafana dashboard (`issue#37867 <http://tracker.ceph.com/issues/37867>`_, `pr#25927 <https://github.com/ceph/ceph/pull/25927>`_, Jason Dillaman)
* rbd-mirror: always attempt to restart canceled status update task (`issue#36500 <http://tracker.ceph.com/issues/36500>`_, `pr#24646 <https://github.com/ceph/ceph/pull/24646>`_, Jason Dillaman)
* rbd-mirror: bootstrap needs to handle local image id collision (`issue#24139 <http://tracker.ceph.com/issues/24139>`_, `pr#22043 <https://github.com/ceph/ceph/pull/22043>`_, Jason Dillaman)
* rbd-mirror: create and export replication perf counters to mgr (`pr#25834 <https://github.com/ceph/ceph/pull/25834>`_, Mykola Golub)
* rbd-mirror: ensure daemon can cleanly exit if pool is deleted (`pr#22348 <https://github.com/ceph/ceph/pull/22348>`_, Jason Dillaman)
* rbd-mirror: ensure remote demotion is replayed locally (`issue#24009 <http://tracker.ceph.com/issues/24009>`_, `pr#21823 <https://github.com/ceph/ceph/pull/21823>`_, Jason Dillaman)
* rbd-mirror: fixed potential crashes during shut down (`issue#24008 <http://tracker.ceph.com/issues/24008>`_, `pr#21817 <https://github.com/ceph/ceph/pull/21817>`_, Jason Dillaman)
* rbd-mirror: guard access to image replayer perf counters (`pr#26097 <https://github.com/ceph/ceph/pull/26097>`_, Mykola Golub)
* rbd-mirror: instantiate the status formatter before changing state (`issue#36084 <http://tracker.ceph.com/issues/36084>`_, `pr#24181 <https://github.com/ceph/ceph/pull/24181>`_, Jason Dillaman)
* rbd-mirror: optionally extract peer secrets from config-key (`issue#24688 <http://tracker.ceph.com/issues/24688>`_, `pr#24036 <https://github.com/ceph/ceph/pull/24036>`_, Jason Dillaman)
* rbd-mirror: optionally support active/active replication (`pr#21915 <https://github.com/ceph/ceph/pull/21915>`_, Mykola Golub, Jason Dillaman)
* rbd-mirror: potential deadlock when running asok 'flush' command (`issue#24141 <http://tracker.ceph.com/issues/24141>`_, `pr#22027 <https://github.com/ceph/ceph/pull/22027>`_, Mykola Golub)
* rbd-mirror: prevent creation of clones when parents are syncing (`issue#24140 <http://tracker.ceph.com/issues/24140>`_, `pr#24063 <https://github.com/ceph/ceph/pull/24063>`_, Jason Dillaman)
* rbd-mirror: schedule rebalancer to level-load instances (`issue#24161 <http://tracker.ceph.com/issues/24161>`_, `pr#22304 <https://github.com/ceph/ceph/pull/22304>`_, Venky Shankar)
* rbd-mirror: update mirror status when stopping (`issue#36659 <http://tracker.ceph.com/issues/36659>`_, `pr#24864 <https://github.com/ceph/ceph/pull/24864>`_, Jason Dillaman)
* rbd-mirror: use active/active policy by default (`issue#38453 <http://tracker.ceph.com/issues/38453>`_, `pr#26603 <https://github.com/ceph/ceph/pull/26603>`_, Jason Dillaman)
* rbd: move image to trash as first step when removing (`issue#24226 <http://tracker.ceph.com/issues/24226>`_, `issue#38404 <http://tracker.ceph.com/issues/38404>`_, `pr#25438 <https://github.com/ceph/ceph/pull/25438>`_, Mahati Chamarthy, Jason Dillaman)
* rbd-nbd: do not ceph_abort() after print the usages (`issue#36660 <http://tracker.ceph.com/issues/36660>`_, `pr#24815 <https://github.com/ceph/ceph/pull/24815>`_, Shiyang Ruan)
* rbd-nbd: support namespaces (`issue#24609 <http://tracker.ceph.com/issues/24609>`_, `pr#25260 <https://github.com/ceph/ceph/pull/25260>`_, Mykola Golub)
* rbd: not allowed to restore an image when it is being deleted (`issue#25346 <http://tracker.ceph.com/issues/25346>`_, `pr#24078 <https://github.com/ceph/ceph/pull/24078>`_, songweibin)
* rbd: online re-sparsify of images (`pr#26226 <https://github.com/ceph/ceph/pull/26226>`_, Mykola Golub)
* rbd: pybind/rbd: add namespace helper API methods (`issue#36622 <http://tracker.ceph.com/issues/36622>`_, `pr#25206 <https://github.com/ceph/ceph/pull/25206>`_, Jason Dillaman)
* rbd: qa/workunits: fixed mon address parsing for rbd-mirror (`issue#38385 <http://tracker.ceph.com/issues/38385>`_, `pr#26521 <https://github.com/ceph/ceph/pull/26521>`_, Jason Dillaman)
* rbd:  rbd: fix error parse arg when getting key (`pr#25152 <https://github.com/ceph/ceph/pull/25152>`_, songweibin)
* rbd: rbd-fuse: look for ceph.conf in standard locations (`issue#12219 <http://tracker.ceph.com/issues/12219>`_, `pr#20598 <https://github.com/ceph/ceph/pull/20598>`_, Jason Dillaman)
* rbd: rbd-fuse: namespace support (`pr#25265 <https://github.com/ceph/ceph/pull/25265>`_, Mykola Golub)
* rbd: rbd-ggate: support namespaces (`issue#24608 <http://tracker.ceph.com/issues/24608>`_, `pr#25266 <https://github.com/ceph/ceph/pull/25266>`_, Mykola Golub)
* rbd: rbd-ggate: tag "level" with need_dynamic (`pr#22557 <https://github.com/ceph/ceph/pull/22557>`_, Kefu Chai)
* rbd: rbd_mirror: assert no requests on destroying InstanceWatcher (`pr#25666 <https://github.com/ceph/ceph/pull/25666>`_, Mykola Golub)
* rbd: rbd_mirror: don't report error if image replay canceled (`pr#25789 <https://github.com/ceph/ceph/pull/25789>`_, Mykola Golub)
* rbd:  rbd-mirror: use pool level config overrides (`pr#24348 <https://github.com/ceph/ceph/pull/24348>`_, Mykola Golub)
* rbd:  rbd: show info about mirror daemon instance in image mirror status output (`pr#24717 <https://github.com/ceph/ceph/pull/24717>`_, Mykola Golub)
* rbd: return error code when the source and distination namespace are different (`pr#24893 <https://github.com/ceph/ceph/pull/24893>`_, Shiyang Ruan)
* rbd: simplified code to remove do_clear_limit function (`pr#23954 <https://github.com/ceph/ceph/pull/23954>`_, Zheng Yin)
* rbd: support namespaces for image migration (`issue#26951 <http://tracker.ceph.com/issues/26951>`_, `pr#24836 <https://github.com/ceph/ceph/pull/24836>`_, Jason Dillaman)
* rbd:  systemd/rbdmap.service: order us before remote-fs-pre.target (`issue#24713 <http://tracker.ceph.com/issues/24713>`_, `pr#22769 <https://github.com/ceph/ceph/pull/22769>`_, Ilya Dryomov)
* rbd: test/librbd: drop unused variable ‘num_aios’ (`pr#23085 <https://github.com/ceph/ceph/pull/23085>`_, songweibin)
* rbd,tests: krbd: alloc_size map option and tests (`pr#26244 <https://github.com/ceph/ceph/pull/26244>`_, Ilya Dryomov)
* rbd,tests: librbd,test: remove unused context_cb() function, silence GCC warnings (`pr#24673 <https://github.com/ceph/ceph/pull/24673>`_, Kefu Chai)
* rbd,tests: pybind/rbd: add assert_raise in test set_snap (`pr#22570 <https://github.com/ceph/ceph/pull/22570>`_, Zheng Yin)
* rbd,tests: qa: krbd_exclusive_option.sh: bump lock_timeout to 60 seconds (`issue#25080 <http://tracker.ceph.com/issues/25080>`_, `pr#22648 <https://github.com/ceph/ceph/pull/22648>`_, Ilya Dryomov)
* rbd,tests: qa: krbd_msgr_segments.t: filter lvcreate output (`pr#22665 <https://github.com/ceph/ceph/pull/22665>`_, Ilya Dryomov)
* rbd,tests: qa: krbd namespaces test (`pr#26339 <https://github.com/ceph/ceph/pull/26339>`_, Ilya Dryomov)
* rbd,tests: qa: objectstore snippets for krbd (`pr#26279 <https://github.com/ceph/ceph/pull/26279>`_, Ilya Dryomov)
* rbd,tests: qa: rbd_workunit_kernel_untar_build: install build dependencies (`issue#35074 <http://tracker.ceph.com/issues/35074>`_, `pr#23840 <https://github.com/ceph/ceph/pull/23840>`_, Ilya Dryomov)
* rbd,tests: qa: rbd/workunits : Replace "rbd bench-write" with "rbd bench --io-type write" (`pr#26168 <https://github.com/ceph/ceph/pull/26168>`_, Shyukri Shyukriev)
* rbd,tests: qa/suites/krbd: more fsx tests (`pr#24354 <https://github.com/ceph/ceph/pull/24354>`_, Ilya Dryomov)
* rbd,tests: qa/suites/rbd: randomly select a supported distro (`pr#22008 <https://github.com/ceph/ceph/pull/22008>`_, Jason Dillaman)
* rbd,tests:  qa/tasks/cram: tasks now must live in the repository (`pr#23976 <https://github.com/ceph/ceph/pull/23976>`_, Ilya Dryomov)
* rbd,tests: qa/tasks/cram: use suite_repo repository for all cram jobs (`pr#23905 <https://github.com/ceph/ceph/pull/23905>`_, Ilya Dryomov)
* rbd,tests: qa/tasks/qemu: use unique clone directory to avoid race with workunit (`issue#36542 <http://tracker.ceph.com/issues/36542>`_, `pr#24696 <https://github.com/ceph/ceph/pull/24696>`_, Jason Dillaman)
* rbd,tests: qa/workunits/rbd: fix cli generic namespace test (`pr#24457 <https://github.com/ceph/ceph/pull/24457>`_, Mykola Golub)
* rbd,tests: qa/workunits/rbd: force v2 image format for namespace test (`pr#24512 <https://github.com/ceph/ceph/pull/24512>`_, Mykola Golub)
* rbd,tests: qa/workunits/rbd: replace usage of 'rados mkpool' (`pr#23938 <https://github.com/ceph/ceph/pull/23938>`_, Jason Dillaman)
* rbd,tests: qa/workunits: replace 'realpath' with 'readlink -f' in fsstress.sh (`issue#36409 <http://tracker.ceph.com/issues/36409>`_, `pr#24550 <https://github.com/ceph/ceph/pull/24550>`_, Jason Dillaman)
* rbd,tests: test/cli-integration/rbd: added new parent image attributes (`pr#25415 <https://github.com/ceph/ceph/pull/25415>`_, Jason Dillaman)
* rbd,tests: test/librados_test_stub: deterministically load cls shared libraries (`pr#21524 <https://github.com/ceph/ceph/pull/21524>`_, Jason Dillaman)
* rbd,tests: test/librados_test_stub: handle object doesn't exist gracefully (`pr#25667 <https://github.com/ceph/ceph/pull/25667>`_, Mykola Golub)
* rbd,tests: test/librbd: fix compiler -Wsign-compare warnings (`pr#23657 <https://github.com/ceph/ceph/pull/23657>`_, Mykola Golub)
* rbd,tests: test/librbd: fix gmock warning in snapshot rollback test (`pr#23736 <https://github.com/ceph/ceph/pull/23736>`_, Jason Dillaman)
* rbd,tests: test/librbd: fix gmock warning in TestMockIoImageRequestWQ.AcquireLockError (`pr#22778 <https://github.com/ceph/ceph/pull/22778>`_, Mykola Golub)
* rbd,tests: test/librbd: fix gmock warnings for get_modify_timestamp call (`pr#23707 <https://github.com/ceph/ceph/pull/23707>`_, Mykola Golub)
* rbd,tests: test/librbd: fix 'Uninteresting mock function call' warning (`pr#26322 <https://github.com/ceph/ceph/pull/26322>`_, Mykola Golub)
* rbd,tests:  test/librbd: fix valgrind warnings (`pr#23827 <https://github.com/ceph/ceph/pull/23827>`_, Mykola Golub)
* rbd,tests: test/librbd: fix -Wsign-compare warnings (`pr#23608 <https://github.com/ceph/ceph/pull/23608>`_, Kefu Chai)
* rbd,tests: test/librbd: metadata key for config should be prefixed with ``conf_`` (`pr#25209 <https://github.com/ceph/ceph/pull/25209>`_, runsisi)
* rbd,tests: test/librbd: migration supporting namespace tests (`pr#24919 <https://github.com/ceph/ceph/pull/24919>`_, Mykola Golub)
* rbd,tests: test/librbd: migration tests did not delete additional pool (`pr#24009 <https://github.com/ceph/ceph/pull/24009>`_, Mykola Golub)
* rbd,tests: test: move OpenStack devstack test to rocky release (`issue#36410 <http://tracker.ceph.com/issues/36410>`_, `pr#24563 <https://github.com/ceph/ceph/pull/24563>`_, Jason Dillaman)
* rbd,tests: test/pybind: fix test_rbd.TestClone.test_trash_snapshot (`issue#25114 <http://tracker.ceph.com/issues/25114>`_, `pr#23256 <https://github.com/ceph/ceph/pull/23256>`_, Mykola Golub)
* rbd,tests: test/pybind/test_rbd: filter out unknown list_children2 keys (`issue#37729 <http://tracker.ceph.com/issues/37729>`_, `pr#25832 <https://github.com/ceph/ceph/pull/25832>`_, Mykola Golub)
* rbd,tests: test/rbd-mirror: disable use of gtest-parallel (`pr#22694 <https://github.com/ceph/ceph/pull/22694>`_, Jason Dillaman)
* rbd,tests:  test/rbd_mirror: fix gmock warnings (`pr#25863 <https://github.com/ceph/ceph/pull/25863>`_, Mykola Golub)
* rbd,tests: test/rbd_mirror: race in TestMockImageMap.AddInstancePingPongImageTest (`issue#36683 <http://tracker.ceph.com/issues/36683>`_, `pr#24897 <https://github.com/ceph/ceph/pull/24897>`_, Mykola Golub)
* rbd,tests: test/rbd_mirror: race in WaitingOnLeaderReleaseLeader (`issue#36236 <http://tracker.ceph.com/issues/36236>`_, `pr#24300 <https://github.com/ceph/ceph/pull/24300>`_, Mykola Golub)
* rbd,tests: test/rbd_mirror: wait for release leader lock fully complete (`pr#25935 <https://github.com/ceph/ceph/pull/25935>`_, Mykola Golub)
* rbd,tests: test/rbd: rbd_ggate test improvements (`pr#23630 <https://github.com/ceph/ceph/pull/23630>`_, Willem Jan Withagen)
* rbd,tests: test: silence -Wsign-compare warnings (`pr#23655 <https://github.com/ceph/ceph/pull/23655>`_, Kefu Chai)
* rbd: tools/rbd/action: align column headers left (`pr#22566 <https://github.com/ceph/ceph/pull/22566>`_, Sage Weil)
* rbd: tools/rbd: assert(g_ceph_context) not g_conf (`pr#23167 <https://github.com/ceph/ceph/pull/23167>`_, Kefu Chai)
* rbd: tools/rbd: minor fixes for rbd du display (`pr#23311 <https://github.com/ceph/ceph/pull/23311>`_, songweibin)
* rbd,tools: rbd-mirror,common: fix typos in logging messages and comments (`pr#25197 <https://github.com/ceph/ceph/pull/25197>`_, Shiyang Ruan)
* rbd,tools: tools/rbd: assert(g_ceph_context) not g_conf (`pr#23008 <https://github.com/ceph/ceph/pull/23008>`_, Kefu Chai)
* rbd: wait for all io complete when bench is interrupted (`pr#26918 <https://github.com/ceph/ceph/pull/26918>`_, Mykola Golub)
* rbd: workaround for llvm linker problem, avoid std:pair dtor (`pr#25301 <https://github.com/ceph/ceph/pull/25301>`_, Willem Jan Withagen)
* Revert "cephfs-journal-tool: enable purge_queue journal's event comma… (`pr#23465 <https://github.com/ceph/ceph/pull/23465>`_, "Yan, Zheng")
* Revert "ceph-fuse: Delete inode's bufferhead was in Tx state would le… (`pr#21975 <https://github.com/ceph/ceph/pull/21975>`_, "Yan, Zheng")
* rgw: abort_bucket_multiparts() ignores individual NoSuchUpload errors (`issue#35986 <http://tracker.ceph.com/issues/35986>`_, `pr#24110 <https://github.com/ceph/ceph/pull/24110>`_, Casey Bodley)
* rgw: adapt AioThrottle for RGWGetObj (`pr#25208 <https://github.com/ceph/ceph/pull/25208>`_, Casey Bodley)
* rgw: Add append object api (`pr#22755 <https://github.com/ceph/ceph/pull/22755>`_, zhang Shaowen, Zhang Shaowen)
* rgw: add bucket as option when show/trim usage (`pr#23819 <https://github.com/ceph/ceph/pull/23819>`_, lvshuhua)
* rgw: add configurable AWS-compat invalid range get behavior (`issue#24317 <http://tracker.ceph.com/issues/24317>`_, `pr#22231 <https://github.com/ceph/ceph/pull/22231>`_, Matt Benjamin)
* rgw: add curl_low_speed_limit and curl_low_speed_time config to avoid (`pr#23058 <https://github.com/ceph/ceph/pull/23058>`_, Mark Kogan, Zhang Shaowen)
* rgw: add Http header 'Server' in response headers (`pr#23282 <https://github.com/ceph/ceph/pull/23282>`_, Zhang Shaowen)
* rgw: Adding documentation for Roles (`pr#24714 <https://github.com/ceph/ceph/pull/24714>`_, Pritha Srivastava)
* rgw: add latency info in the log of req done (`pr#23906 <https://github.com/ceph/ceph/pull/23906>`_, lvshuhua)
* rgw: add list user admin OP API (`pr#25073 <https://github.com/ceph/ceph/pull/25073>`_, Oshyn Song)
* rgw: add --op-mask in radosgw-admin help info (`pr#24848 <https://github.com/ceph/ceph/pull/24848>`_, yuliyang)
* rgw: add optional_yield to block_while_resharding() (`pr#25357 <https://github.com/ceph/ceph/pull/25357>`_, Casey Bodley)
* rgw: add option for relaxed region enforcement (`issue#24507 <http://tracker.ceph.com/issues/24507>`_, `pr#22533 <https://github.com/ceph/ceph/pull/22533>`_, Matt Benjamin)
* rgw: Add rgw xml unit tests (`pr#26682 <https://github.com/ceph/ceph/pull/26682>`_, Yuval Lifshitz)
* rgw: add s3 notification sub resources (`pr#23405 <https://github.com/ceph/ceph/pull/23405>`_, yuliyang)
* rgw: admin rest api support op-mask (`pr#24869 <https://github.com/ceph/ceph/pull/24869>`_, yuliyang)
* rgw: admin/user ops dump user 'system' flag (`pr#17414 <https://github.com/ceph/ceph/pull/17414>`_, fang.yuxiang)
* rgw: All Your Fault (`issue#24962 <http://tracker.ceph.com/issues/24962>`_, `pr#23099 <https://github.com/ceph/ceph/pull/23099>`_, Adam C. Emerson)
* rgw: apply quota config to users created via external auth (`issue#24595 <http://tracker.ceph.com/issues/24595>`_, `pr#24177 <https://github.com/ceph/ceph/pull/24177>`_, Casey Bodley)
* rgw: archive zone (`pr#25137 <https://github.com/ceph/ceph/pull/25137>`_, Yehuda Sadeh, Javier M. Mellid)
* rgw: async sync_object and remove_object does not access coroutine me… (`issue#35905 <http://tracker.ceph.com/issues/35905>`_, `pr#24007 <https://github.com/ceph/ceph/pull/24007>`_, Tianshan Qu)
* rgw: async watch registration (`pr#21838 <https://github.com/ceph/ceph/pull/21838>`_, Yehuda Sadeh)
* rgw: avoid race condition in RGWHTTPClient::wait() (`pr#21767 <https://github.com/ceph/ceph/pull/21767>`_, cfanz)
* rgw: beast frontend logs socket errors at level 4 (`pr#24677 <https://github.com/ceph/ceph/pull/24677>`_, Casey Bodley)
* rgw: beast frontend parses ipv6 addrs (`issue#36662 <http://tracker.ceph.com/issues/36662>`_, `pr#24887 <https://github.com/ceph/ceph/pull/24887>`_, Casey Bodley)
* rgw: beast frontend reworks pause/stop and yields during body io (`pr#21271 <https://github.com/ceph/ceph/pull/21271>`_, Casey Bodley)
* rgw: bucket full sync handles delete markers (`issue#38007 <http://tracker.ceph.com/issues/38007>`_, `pr#26081 <https://github.com/ceph/ceph/pull/26081>`_, Casey Bodley)
* rgw: bucket limit check misbehaves for > max-entries buckets (usually… (`pr#26800 <https://github.com/ceph/ceph/pull/26800>`_, Matt Benjamin)
* rgw: bucket sync status improvements, part 1 (`pr#21788 <https://github.com/ceph/ceph/pull/21788>`_, Casey Bodley)
* rgw: bug in versioning concurrent, list and get have consistency issue (`pr#26197 <https://github.com/ceph/ceph/pull/26197>`_, Wang Hao)
* rgw: catch exceptions from librados::NObjectIterator (`issue#37091 <http://tracker.ceph.com/issues/37091>`_, `pr#25081 <https://github.com/ceph/ceph/pull/25081>`_, Casey Bodley)
* rgw: change default rgw_thread_pool_size to 512 (`issue#24544 <http://tracker.ceph.com/issues/24544>`_, `pr#22581 <https://github.com/ceph/ceph/pull/22581>`_, Douglas Fuller)
* rgw: change the "rgw admin status" 'num_shards' output to signed int (`issue#37645 <http://tracker.ceph.com/issues/37645>`_, `pr#25538 <https://github.com/ceph/ceph/pull/25538>`_, Mark Kogan)
* rgw: check for non-existent bucket in RGWGetACLs (`pr#26212 <https://github.com/ceph/ceph/pull/26212>`_, Matt Benjamin)
* rgw: civetweb: update for url validation fixes (`issue#24158 <http://tracker.ceph.com/issues/24158>`_, `pr#22054 <https://github.com/ceph/ceph/pull/22054>`_, Abhishek Lekshmanan)
* rgw: civetweb: use poll instead of select while waiting on sockets (`issue#24364 <http://tracker.ceph.com/issues/24364>`_, `pr#24027 <https://github.com/ceph/ceph/pull/24027>`_, Abhishek Lekshmanan)
* rgw: clean-up -- insure C++ source code files contain editor directives (`pr#25495 <https://github.com/ceph/ceph/pull/25495>`_, J. Eric Ivancich)
* rgw: cleanups for sync tracing (`pr#23828 <https://github.com/ceph/ceph/pull/23828>`_, Casey Bodley)
* rgw: clean-up -- use enum class for stats category (`pr#25450 <https://github.com/ceph/ceph/pull/25450>`_, J. Eric Ivancich)
* rgw: cls/rgw: don't assert in decode_list_index_key() (`issue#24117 <http://tracker.ceph.com/issues/24117>`_, `pr#22440 <https://github.com/ceph/ceph/pull/22440>`_, Yehuda Sadeh)
* rgw: cls/rgw: raise debug level of bi_log_iterate_entries output (`pr#25570 <https://github.com/ceph/ceph/pull/25570>`_, Casey Bodley)
* rgw: cls/user: cls_user_remove_bucket writes modified header (`issue#36496 <http://tracker.ceph.com/issues/36496>`_, `pr#24645 <https://github.com/ceph/ceph/pull/24645>`_, Casey Bodley)
* rgw: Code for STS Authentication (`pr#23504 <https://github.com/ceph/ceph/pull/23504>`_, Pritha Srivastava)
* rgw: common/options: correct the description of rgw_enable_lc_threads option (`pr#23511 <https://github.com/ceph/ceph/pull/23511>`_, excellentkf)
* rgw: continue enoent index in dir_suggest (`issue#24640 <http://tracker.ceph.com/issues/24640>`_, `pr#22937 <https://github.com/ceph/ceph/pull/22937>`_, Tianshan Qu)
* rgw: copy actual stats from the source shards during reshard (`issue#36290 <http://tracker.ceph.com/issues/36290>`_, `pr#24444 <https://github.com/ceph/ceph/pull/24444>`_, Abhishek Lekshmanan)
* rgw: Copying object data should generate new tail tag for the new object (`issue#24562 <http://tracker.ceph.com/issues/24562>`_, `pr#22613 <https://github.com/ceph/ceph/pull/22613>`_, Zhang Shaowen)
* rgw: Correcting logic for signature calculation for non s3 ops (`pr#26098 <https://github.com/ceph/ceph/pull/26098>`_, Pritha Srivastava)
* rgw: cors rules num limit (`pr#23434 <https://github.com/ceph/ceph/pull/23434>`_, yuliyang)
* rgw: crypto: add openssl support for RGW encryption (`pr#15168 <https://github.com/ceph/ceph/pull/15168>`_, Qiaowei Ren)
* rgw: data sync accepts ERR_PRECONDITION_FAILED on remove_object() (`issue#37448 <http://tracker.ceph.com/issues/37448>`_, `pr#25310 <https://github.com/ceph/ceph/pull/25310>`_, Casey Bodley)
* rgw: data sync drains lease stack on lease failure (`issue#38479 <http://tracker.ceph.com/issues/38479>`_, `pr#26639 <https://github.com/ceph/ceph/pull/26639>`_, Casey Bodley)
* rgw: data sync respects error_retry_time for backoff on error_repo (`issue#26938 <http://tracker.ceph.com/issues/26938>`_, `pr#23571 <https://github.com/ceph/ceph/pull/23571>`_, Casey Bodley)
* rgw: delete multi object num limit (`pr#23544 <https://github.com/ceph/ceph/pull/23544>`_, yuliyang)
* rgw: delete some unused code about std::regex (`pr#23221 <https://github.com/ceph/ceph/pull/23221>`_, Xueyu Bai)
* rgw: [DNM] rgw: Controlling STS authentication via a Policy (`pr#24818 <https://github.com/ceph/ceph/pull/24818>`_, Pritha Srivastava)
* rgw: do not ignore EEXIST in RGWPutObj::execute (`issue#22790 <http://tracker.ceph.com/issues/22790>`_, `pr#23033 <https://github.com/ceph/ceph/pull/23033>`_, Matt Benjamin)
* rgw: Do not modify email if argument is not set (`pr#22024 <https://github.com/ceph/ceph/pull/22024>`_, Volker Theile)
* rgw: dont access rgw_http_req_data::client of canceled request (`issue#35851 <http://tracker.ceph.com/issues/35851>`_, `pr#23988 <https://github.com/ceph/ceph/pull/23988>`_, Casey Bodley)
* rgw: Don't treat colons specially when matching resource field of ARNs in S3 Policy (`issue#23817 <http://tracker.ceph.com/issues/23817>`_, `pr#25145 <https://github.com/ceph/ceph/pull/25145>`_, Adam C. Emerson)
* rgw: drop unused tmp in main() (`pr#23899 <https://github.com/ceph/ceph/pull/23899>`_, luomuyao)
* rgw: escape markers in RGWOp_Metadata_List::execute (`issue#23099 <http://tracker.ceph.com/issues/23099>`_, `pr#22721 <https://github.com/ceph/ceph/pull/22721>`_, Matt Benjamin)
* rgw: ES sync: be more restrictive on object system attrs (`issue#36233 <http://tracker.ceph.com/issues/36233>`_, `pr#24492 <https://github.com/ceph/ceph/pull/24492>`_, Abhishek Lekshmanan)
* rgw: etag in rgw copy result response body rather in header (`pr#23751 <https://github.com/ceph/ceph/pull/23751>`_, yuliyang)
* rgw: feature -- log successful bucket resharding events (`pr#25510 <https://github.com/ceph/ceph/pull/25510>`_, J. Eric Ivancich)
* rgw: fetch_remote_obj filters out olh attrs (`issue#37792 <http://tracker.ceph.com/issues/37792>`_, `pr#25794 <https://github.com/ceph/ceph/pull/25794>`_, Casey Bodley)
* rgw: fix bad user stats on versioned bucket after reshard (`pr#25414 <https://github.com/ceph/ceph/pull/25414>`_, J. Eric Ivancich)
* rgw: fix build (`pr#22194 <https://github.com/ceph/ceph/pull/22194>`_, Yehuda Sadeh)
* rgw: fix build (`pr#23248 <https://github.com/ceph/ceph/pull/23248>`_, Matt Benjamin)
* rgw: fix chunked-encoding for chunks >1MiB (`issue#35990 <http://tracker.ceph.com/issues/35990>`_, `pr#24114 <https://github.com/ceph/ceph/pull/24114>`_, Robin H. Johnson)
* rgw: fix compilation after pubsub conflict (`pr#25568 <https://github.com/ceph/ceph/pull/25568>`_, Casey Bodley)
* rgw: fix copy response header etag format not correct (`issue#24563 <http://tracker.ceph.com/issues/24563>`_, `pr#22614 <https://github.com/ceph/ceph/pull/22614>`_, Tianshan Qu)
* rgw: fix CreateBucket with BucketLocation parameter failed under default zonegroup (`pr#22312 <https://github.com/ceph/ceph/pull/22312>`_, Enming Zhang)
* rgw: fix deadlock on RGWIndexCompletionManager::stop (`issue#26949 <http://tracker.ceph.com/issues/26949>`_, `pr#23590 <https://github.com/ceph/ceph/pull/23590>`_, Yao Zongyou)
* rgw: fix dependencies/target_link_libraries (`pr#23056 <https://github.com/ceph/ceph/pull/23056>`_, Michal Jarzabek)
* rgw: fixes for sync of versioned objects (`issue#24367 <http://tracker.ceph.com/issues/24367>`_, `pr#22347 <https://github.com/ceph/ceph/pull/22347>`_, Casey Bodley)
* rgw: Fixes to permission evaluation related to user policies (`pr#25180 <https://github.com/ceph/ceph/pull/25180>`_, Pritha Srivastava)
* rgw: fix Etag error in multipart copy response (`pr#23749 <https://github.com/ceph/ceph/pull/23749>`_, yuliyang)
* rgw: Fix for buffer overflow in STS op_post() (`issue#36579 <http://tracker.ceph.com/issues/36579>`_, `pr#24510 <https://github.com/ceph/ceph/pull/24510>`_, Pritha Srivastava, Marcus Watts)
* rgw: Fix for SignatureMismatchError in s3 commands (`pr#26204 <https://github.com/ceph/ceph/pull/26204>`_, Pritha Srivastava)
* rgw: fix FTBFS introduced by abca9805 (`pr#23046 <https://github.com/ceph/ceph/pull/23046>`_, Kefu Chai)
* rgw: fix index complete miss zones_trace set (`issue#24590 <http://tracker.ceph.com/issues/24590>`_, `pr#22632 <https://github.com/ceph/ceph/pull/22632>`_, Tianshan Qu)
* rgw: fix index update in dir_suggest_changes (`issue#24280 <http://tracker.ceph.com/issues/24280>`_, `pr#22217 <https://github.com/ceph/ceph/pull/22217>`_, Tianshan Qu)
* rgw: fix ldap secret parsing (`pr#25796 <https://github.com/ceph/ceph/pull/25796>`_, Matt Benjamin)
* rgw: fix leak of curl handle on shutdown (`issue#35715 <http://tracker.ceph.com/issues/35715>`_, `pr#23986 <https://github.com/ceph/ceph/pull/23986>`_, Casey Bodley)
* rgw: Fix log level of gc_iterate_entries (`issue#23801 <http://tracker.ceph.com/issues/23801>`_, `pr#22868 <https://github.com/ceph/ceph/pull/22868>`_, iliul)
* rgw: fix max-size in radosgw-admin and REST Admin API (`pr#24062 <https://github.com/ceph/ceph/pull/24062>`_, Nick Erdmann)
* rgw: fix meta and data notify thread miss stop cr manager (`issue#24589 <http://tracker.ceph.com/issues/24589>`_, `pr#22631 <https://github.com/ceph/ceph/pull/22631>`_, Tianshan Qu)
* rgw: fix obj can still be deleted even if deleteobject policy is set (`issue#37403 <http://tracker.ceph.com/issues/37403>`_, `pr#25278 <https://github.com/ceph/ceph/pull/25278>`_, Enming.Zhang)
* rgw: fix radosgw-admin build error (`pr#21599 <https://github.com/ceph/ceph/pull/21599>`_, cfanz)
* rgw: fix rgw_data_sync_info::json_decode() (`issue#38373 <http://tracker.ceph.com/issues/38373>`_, `pr#26494 <https://github.com/ceph/ceph/pull/26494>`_, Casey Bodley)
* rgw: fix RGWSyncTraceNode crash in reload (`issue#24432 <http://tracker.ceph.com/issues/24432>`_, `pr#22432 <https://github.com/ceph/ceph/pull/22432>`_, Tianshan Qu)
* rgw: fix stats for versioned buckets after reshard (`pr#25333 <https://github.com/ceph/ceph/pull/25333>`_, J. Eric Ivancich)
* rgw: fix uninitialized access (`pr#25002 <https://github.com/ceph/ceph/pull/25002>`_, Yehuda Sadeh)
* rgw: fix unordered bucket listing when object names are adorned (`issue#38486 <http://tracker.ceph.com/issues/38486>`_, `pr#26658 <https://github.com/ceph/ceph/pull/26658>`_, J. Eric Ivancich)
* rgw: fix vector index out of range in RGWReadDataSyncRecoveringShardsCR (`issue#36537 <http://tracker.ceph.com/issues/36537>`_, `pr#24680 <https://github.com/ceph/ceph/pull/24680>`_, Casey Bodley)
* rgw: fix version bucket stats (`issue#21429 <http://tracker.ceph.com/issues/21429>`_, `pr#17789 <https://github.com/ceph/ceph/pull/17789>`_, Shasha Lu)
* rgw: fix versioned obj copy generating tags (`issue#37588 <http://tracker.ceph.com/issues/37588>`_, `pr#25473 <https://github.com/ceph/ceph/pull/25473>`_, Abhishek Lekshmanan)
* rgw: fix wrong debug related to user ACLs in rgw_build_bucket_policies() (`issue#19514 <http://tracker.ceph.com/issues/19514>`_, `pr#14369 <https://github.com/ceph/ceph/pull/14369>`_, Radoslaw Zarzynski)
* rgw: get or set realm zonegroup zone need check user's caps (`pr#25178 <https://github.com/ceph/ceph/pull/25178>`_, yuliyang, Casey Bodley)
* rgw: Get the user metadata of the user used to sign the request (`pr#22390 <https://github.com/ceph/ceph/pull/22390>`_, Volker Theile)
* rgw: handle cases around zone deletion (`issue#37328 <http://tracker.ceph.com/issues/37328>`_, `pr#25160 <https://github.com/ceph/ceph/pull/25160>`_, Abhishek Lekshmanan)
* rgw: handle S3 version 2 pre-signed urls with meta-data (`pr#24683 <https://github.com/ceph/ceph/pull/24683>`_, Matt Benjamin)
* rgw: have a configurable authentication order (`issue#23089 <http://tracker.ceph.com/issues/23089>`_, `pr#21494 <https://github.com/ceph/ceph/pull/21494>`_, Abhishek Lekshmanan)
* rgw: http client: print curl error messages during curl failures (`pr#23318 <https://github.com/ceph/ceph/pull/23318>`_, Abhishek Lekshmanan)
* rgw: Improvements to STS Lite documentation (`pr#24847 <https://github.com/ceph/ceph/pull/24847>`_, Pritha Srivastava)
* rgw: Initial commit for AssumeRoleWithWebIdentity (`pr#26002 <https://github.com/ceph/ceph/pull/26002>`_, Pritha Srivastava)
* rgw: initial RGWRados refactoring work (`pr#24014 <https://github.com/ceph/ceph/pull/24014>`_, Yehuda Sadeh, Casey Bodley)
* rgw: Initial work for OPA-Ceph integration (`pr#22624 <https://github.com/ceph/ceph/pull/22624>`_, Ashutosh Narkar)
* rgw: librgw: initialize curl and http client for multisite (`issue#36302 <http://tracker.ceph.com/issues/36302>`_, `pr#24402 <https://github.com/ceph/ceph/pull/24402>`_, Casey Bodley)
* rgw: librgw: support symbolic link (`pr#19684 <https://github.com/ceph/ceph/pull/19684>`_, Tao Chen)
* rgw: lifcycle: don't reject compound rules with empty prefix (`issue#37879 <http://tracker.ceph.com/issues/37879>`_, `pr#25926 <https://github.com/ceph/ceph/pull/25926>`_, Matt Benjamin)
* rgw: Limit the number of lifecycle rules on one bucket (`issue#24572 <http://tracker.ceph.com/issues/24572>`_, `pr#22623 <https://github.com/ceph/ceph/pull/22623>`_, Zhang Shaowen)
* rgw: list bucket can not show the object uploaded by RGWPostObj when enable bucket versioning (`pr#24341 <https://github.com/ceph/ceph/pull/24341>`_, yuliyang)
* rgw: log http status with op prefix if available (`pr#25102 <https://github.com/ceph/ceph/pull/25102>`_, Casey Bodley)
* rgw: log refactoring for data sync (`pr#23843 <https://github.com/ceph/ceph/pull/23843>`_, Casey Bodley)
* rgw: log refactoring for meta sync (`pr#23950 <https://github.com/ceph/ceph/pull/23950>`_, Casey Bodley, Ali Maredia)
* rgw: make beast the default for rgw_frontends (`pr#26599 <https://github.com/ceph/ceph/pull/26599>`_, Casey Bodley)
* rgw: Minor fixes to AssumeRole for boto compliance (`pr#24845 <https://github.com/ceph/ceph/pull/24845>`_, Pritha Srivastava)
* rgw: Minor fixes to radosgw-admin commands for a role (`pr#24730 <https://github.com/ceph/ceph/pull/24730>`_, Pritha Srivastava)
* rgw: move all reshard config options out of legacy_config_options (`pr#25356 <https://github.com/ceph/ceph/pull/25356>`_, J. Eric Ivancich)
* rgw: move keystone secrets from ceph.conf to files (`issue#36621 <http://tracker.ceph.com/issues/36621>`_, `pr#24816 <https://github.com/ceph/ceph/pull/24816>`_, Matt Benjamin)
* rgw: multiple es related fixes and improvements (`issue#22877 <http://tracker.ceph.com/issues/22877>`_, `issue#38028 <http://tracker.ceph.com/issues/38028>`_, `issue#38030 <http://tracker.ceph.com/issues/38030>`_, `issue#36092 <http://tracker.ceph.com/issues/36092>`_, `pr#26106 <https://github.com/ceph/ceph/pull/26106>`_, Yehuda Sadeh, Abhishek Lekshmanan)
* rgw: need to give a type in list constructor (`pr#25161 <https://github.com/ceph/ceph/pull/25161>`_, Willem Jan Withagen)
* rgw: new librgw_admin_us (`pr#21439 <https://github.com/ceph/ceph/pull/21439>`_, Orit Wasserman, Matt Benjamin)
* rgw: policy: fix NotAction, NotPricipal, NotResource does not take effect (`pr#23625 <https://github.com/ceph/ceph/pull/23625>`_, xiangxiang)
* rgw: policy: fix s3:x-amz-grant-read-acp keyword error (`pr#23610 <https://github.com/ceph/ceph/pull/23610>`_, xiangxiang)
* rgw: policy: modify some operation permission keyword (`issue#24061 <http://tracker.ceph.com/issues/24061>`_, `pr#20974 <https://github.com/ceph/ceph/pull/20974>`_, xiangxiang)
* rgw: pub-sub (`pr#23298 <https://github.com/ceph/ceph/pull/23298>`_, Yehuda Sadeh)
* rgw: qa/suites/rgw/verify/tasks/cls_rgw: test cls_rgw (`pr#22919 <https://github.com/ceph/ceph/pull/22919>`_, Sage Weil)
* rgw: radogw-admin reshard status command should print text for reshard status (`issue#23257 <http://tracker.ceph.com/issues/23257>`_, `pr#20779 <https://github.com/ceph/ceph/pull/20779>`_, Orit Wasserman)
* rgw: radosgw-admin: add mfa related command and options (`pr#23416 <https://github.com/ceph/ceph/pull/23416>`_, Enming.Zhang)
* rgw: `radosgw-admin bucket rm ... --purge-objects` can hang (`issue#38134 <http://tracker.ceph.com/issues/38134>`_, `pr#26231 <https://github.com/ceph/ceph/pull/26231>`_, J. Eric Ivancich)
* rgw: "radosgw-admin objects expire" always returns ok even if the process fails (`issue#24592 <http://tracker.ceph.com/issues/24592>`_, `pr#22635 <https://github.com/ceph/ceph/pull/22635>`_, Zhang Shaowen)
* rgw: radosgw-admin: 'sync error trim' loops until complete (`issue#24873 <http://tracker.ceph.com/issues/24873>`_, `pr#23032 <https://github.com/ceph/ceph/pull/23032>`_, Casey Bodley)
* rgw: radosgw-admin: translate reshard status codes (trivial) (`issue#36486 <http://tracker.ceph.com/issues/36486>`_, `pr#24638 <https://github.com/ceph/ceph/pull/24638>`_, Matt Benjamin)
* rgw: RADOS::Obj::operate takes optional_yield (`pr#25068 <https://github.com/ceph/ceph/pull/25068>`_, Casey Bodley)
* rgw: rados tiering (`issue#19510 <http://tracker.ceph.com/issues/19510>`_, `pr#25774 <https://github.com/ceph/ceph/pull/25774>`_, yuliyang, Yehuda Sadeh, Zhang Shaowen)
* rgw: raise debug level on redundant data sync error messages (`issue#35830 <http://tracker.ceph.com/issues/35830>`_, `pr#23981 <https://github.com/ceph/ceph/pull/23981>`_, Casey Bodley)
* rgw: raise default rgw_curl_low_speed_time to 300 seconds (`issue#27989 <http://tracker.ceph.com/issues/27989>`_, `pr#23759 <https://github.com/ceph/ceph/pull/23759>`_, Casey Bodley)
* rgw: refactor logging in gc and lc (`pr#24530 <https://github.com/ceph/ceph/pull/24530>`_, Ali Maredia)
* rgw: refactor PutObjProcessor stack (`pr#24453 <https://github.com/ceph/ceph/pull/24453>`_, Casey Bodley)
* rgw: reject invalid methods in validate_cors_rule_method (`issue#24223 <http://tracker.ceph.com/issues/24223>`_, `pr#22145 <https://github.com/ceph/ceph/pull/22145>`_, Jeegn Chen)
* rgw: remove all traces of cls replica_log (`pr#21680 <https://github.com/ceph/ceph/pull/21680>`_, Casey Bodley)
* rgw: remove duplicated ``RGWRados::list_buckets_`` helpers (`pr#25240 <https://github.com/ceph/ceph/pull/25240>`_, Casey Bodley)
* rgw: remove expired entries from the cache (`issue#23379 <http://tracker.ceph.com/issues/23379>`_, `pr#22410 <https://github.com/ceph/ceph/pull/22410>`_, Mark Kogan)
* rgw: remove repetitive conditional statement in RGWHandler_REST_Obj_S3 (`pr#24162 <https://github.com/ceph/ceph/pull/24162>`_, Zhang Shaowen)
* rgw: remove rgw_aclparser.cc (`issue#36665 <http://tracker.ceph.com/issues/36665>`_, `pr#24866 <https://github.com/ceph/ceph/pull/24866>`_, Matt Benjamin)
* rgw: remove the useless is_cors_op in RGWHandler_REST_Obj_S3 (`pr#22114 <https://github.com/ceph/ceph/pull/22114>`_, Zhang Shaowen)
* rgw: remove unused aio helper functions (`pr#25239 <https://github.com/ceph/ceph/pull/25239>`_, Casey Bodley)
* rgw: renew resharding locks to prevent expiration (`issue#27219 <http://tracker.ceph.com/issues/27219>`_, `issue#34307 <http://tracker.ceph.com/issues/34307>`_, `pr#24406 <https://github.com/ceph/ceph/pull/24406>`_, Orit Wasserman, J. Eric Ivancich)
* rgw: repair olh attributes that were broken by sync (`issue#37792 <http://tracker.ceph.com/issues/37792>`_, `pr#26157 <https://github.com/ceph/ceph/pull/26157>`_, Casey Bodley)
* rgw: require --yes-i-really-mean-it to run radosgw-admin orphans find (`issue#24146 <http://tracker.ceph.com/issues/24146>`_, `pr#22036 <https://github.com/ceph/ceph/pull/22036>`_, Matt Benjamin)
* rgw: reshard add: fail correctly on a non existant bucket (`issue#36449 <http://tracker.ceph.com/issues/36449>`_, `pr#24594 <https://github.com/ceph/ceph/pull/24594>`_, Abhishek Lekshmanan)
* rgw: reshard clean-up and associated commits (`pr#25142 <https://github.com/ceph/ceph/pull/25142>`_, J. Eric Ivancich)
* rgw: reshard improvements (`pr#25003 <https://github.com/ceph/ceph/pull/25003>`_, J. Eric Ivancich)
* rgw: reshard stale instance cleanup (`issue#24082 <http://tracker.ceph.com/issues/24082>`_, `pr#24662 <https://github.com/ceph/ceph/pull/24662>`_, Abhishek Lekshmanan)
* rgw: resolve bugs and clean up garbage collection code (`issue#38454 <http://tracker.ceph.com/issues/38454>`_, `pr#26601 <https://github.com/ceph/ceph/pull/26601>`_, J. Eric Ivancich)
* rgw: resolve bug where marker was not advanced during garbage collection (`issue#38408 <http://tracker.ceph.com/issues/38408>`_, `pr#26545 <https://github.com/ceph/ceph/pull/26545>`_, J. Eric Ivancich)
* rgw: return err_malformed_xml when MaxAgeSeconds is an invalid integer (`issue#26957 <http://tracker.ceph.com/issues/26957>`_, `pr#23626 <https://github.com/ceph/ceph/pull/23626>`_, Chang Liu)
* rgw: Return tenant field in bucket_stats function (`pr#24895 <https://github.com/ceph/ceph/pull/24895>`_, Volker Theile)
* rgw: return valid Location element, PostObj (`issue#22927 <http://tracker.ceph.com/issues/22927>`_, `pr#20330 <https://github.com/ceph/ceph/pull/20330>`_, yuliyang)
* rgw: return x-amz-version-id: null when delete obj in versioning suspended bucket (`issue#35814 <http://tracker.ceph.com/issues/35814>`_, `pr#23927 <https://github.com/ceph/ceph/pull/23927>`_, yuliyang)
* rgw: Revert "rgw: lifcycle: don't reject compound rules with empty prefix" (`pr#26491 <https://github.com/ceph/ceph/pull/26491>`_, Matt Benjamin)
* rgw: rgw-admin: add "--trim-delay-ms" introduction for 'sync error trim' (`pr#23342 <https://github.com/ceph/ceph/pull/23342>`_, Enming.Zhang)
* rgw: rgw-admin: fix data sync report for master zone (`pr#23925 <https://github.com/ceph/ceph/pull/23925>`_, cfanz)
* rgw: RGWAsyncGetBucketInstanceInfo does not access coroutine memory (`issue#35812 <http://tracker.ceph.com/issues/35812>`_, `pr#23987 <https://github.com/ceph/ceph/pull/23987>`_, Casey Bodley)
* rgw: rgw/beast: drop privileges after binding ports (`issue#36041 <http://tracker.ceph.com/issues/36041>`_, `pr#24271 <https://github.com/ceph/ceph/pull/24271>`_, Paul Emmerich)
* rgw: RGWBucket::link supports tenant (`issue#22666 <http://tracker.ceph.com/issues/22666>`_, `pr#23119 <https://github.com/ceph/ceph/pull/23119>`_, Casey Bodley)
* rgw:     rgw: change the way sysobj filters raw attributes, fix bucket sync state xattrs (`issue#37281 <http://tracker.ceph.com/issues/37281>`_, `pr#25123 <https://github.com/ceph/ceph/pull/25123>`_, Yehuda Sadeh)
* rgw: rgw, cls: remove cls_statelog and rgw opstate tracking (`pr#24059 <https://github.com/ceph/ceph/pull/24059>`_, Casey Bodley)
* rgw: rgw_file: deep stat handling (`issue#24915 <http://tracker.ceph.com/issues/24915>`_, `pr#23038 <https://github.com/ceph/ceph/pull/23038>`_, Matt Benjamin)
* rgw: rgw_file: not check max_objects when creating file (`pr#24846 <https://github.com/ceph/ceph/pull/24846>`_, Tao Chen)
* rgw: rgw_file: use correct secret key to check auth (`pr#26130 <https://github.com/ceph/ceph/pull/26130>`_, MinSheng Lin)
* rgw: rgw_file: user info never synced since librgw init (`pr#25406 <https://github.com/ceph/ceph/pull/25406>`_, Tao Chen)
* rgw: [rgw]: Fix help of radosgw-admin user info in case no uid (`pr#25078 <https://github.com/ceph/ceph/pull/25078>`_, Marc Koderer)
* rgw: rgwgc:process coredump in some special case (`issue#23199 <http://tracker.ceph.com/issues/23199>`_, `pr#25430 <https://github.com/ceph/ceph/pull/25430>`_, zhaokun)
* rgw: rgw multisite: async rados requests don't access coroutine memory (`issue#35543 <http://tracker.ceph.com/issues/35543>`_, `pr#23920 <https://github.com/ceph/ceph/pull/23920>`_, Casey Bodley)
* rgw: rgw multisite: bucket sync transitions back to StateInit on OP_SYNCSTOP (`issue#26895 <http://tracker.ceph.com/issues/26895>`_, `pr#23574 <https://github.com/ceph/ceph/pull/23574>`_, Casey Bodley)
* rgw: rgw multisite: enforce spawn_window for data full sync (`issue#26897 <http://tracker.ceph.com/issues/26897>`_, `pr#23534 <https://github.com/ceph/ceph/pull/23534>`_, Casey Bodley)
* rgw: rgw-multisite: fix endless loop in RGWBucketShardIncrementalSyncCR (`issue#24603 <http://tracker.ceph.com/issues/24603>`_, `pr#22660 <https://github.com/ceph/ceph/pull/22660>`_, cfanz)
* rgw: rgw multisite: incremental data sync uses truncated flag to detect end of listing (`issue#26952 <http://tracker.ceph.com/issues/26952>`_, `pr#23596 <https://github.com/ceph/ceph/pull/23596>`_, Casey Bodley)
* rgw: rgw multisite: only update last_trim marker on ENODATA (`issue#38075 <http://tracker.ceph.com/issues/38075>`_, `pr#26190 <https://github.com/ceph/ceph/pull/26190>`_, Casey Bodley)
* rgw: rgw multisite: uses local DataChangesLog to track active buckets for trim (`issue#36034 <http://tracker.ceph.com/issues/36034>`_, `pr#24221 <https://github.com/ceph/ceph/pull/24221>`_, Casey Bodley)
* rgw: rgw/pubsub: add amqp push endpoint (`pr#25866 <https://github.com/ceph/ceph/pull/25866>`_, Yuval Lifshitz)
* rgw: rgw/pubsub: add pubsub tests (`pr#26299 <https://github.com/ceph/ceph/pull/26299>`_, Yuval Lifshitz)
* rgw: RGWRadosGetOmapKeysCR takes result by shared_ptr (`issue#21154 <http://tracker.ceph.com/issues/21154>`_, `pr#23634 <https://github.com/ceph/ceph/pull/23634>`_, Casey Bodley)
* rgw: RGWRadosGetOmapKeysCR uses 'more' flag from omap_get_keys2() (`pr#23401 <https://github.com/ceph/ceph/pull/23401>`_, Casey Bodley, Sage Weil)
* rgw: remove duplicate include header files in rgw_rados.cc (`pr#18578 <https://github.com/ceph/ceph/pull/18578>`_, Sibei Gao)
* rgw: rgw_sync: drop ENOENT error logs from mdlog (`pr#26971 <https://github.com/ceph/ceph/pull/26971>`_, Abhishek Lekshmanan)
* rgw: Robustly notify (`issue#24963 <http://tracker.ceph.com/issues/24963>`_, `pr#23100 <https://github.com/ceph/ceph/pull/23100>`_, Adam C. Emerson)
* rgw: s3: awsv4 drop special handling for x-amz-credential (`issue#26965 <http://tracker.ceph.com/issues/26965>`_, `pr#23652 <https://github.com/ceph/ceph/pull/23652>`_, Abhishek Lekshmanan)
* rgw: sanitize customer encryption keys from log output in v4 auth (`issue#37847 <http://tracker.ceph.com/issues/37847>`_, `pr#25881 <https://github.com/ceph/ceph/pull/25881>`_, Casey Bodley)
* rgw: scheduler (`pr#26008 <https://github.com/ceph/ceph/pull/26008>`_, Casey Bodley, Abhishek Lekshmanan)
* rgw: set cr state if aio_read err return in RGWCloneMetaLogCoroutine (`issue#24566 <http://tracker.ceph.com/issues/24566>`_, `pr#22617 <https://github.com/ceph/ceph/pull/22617>`_, Tianshan Qu)
* rgw: set default objecter_inflight_ops = 24576 (`issue#25109 <http://tracker.ceph.com/issues/25109>`_, `pr#23242 <https://github.com/ceph/ceph/pull/23242>`_, Matt Benjamin)
* rgw:  should recode  canonical_uri when caculate s3 v4 auth (`issue#23587 <http://tracker.ceph.com/issues/23587>`_, `pr#21286 <https://github.com/ceph/ceph/pull/21286>`_, yuliyang)
* rgw: some fix for es sync (`issue#23842 <http://tracker.ceph.com/issues/23842>`_, `issue#23841 <http://tracker.ceph.com/issues/23841>`_, `pr#21622 <https://github.com/ceph/ceph/pull/21622>`_, Tianshan Qu, Shang Ding)
* rgw: support admin rest api get user info through user's access-key (`pr#22790 <https://github.com/ceph/ceph/pull/22790>`_, yuliyang)
* rgw: support server-side encryption when SSL is terminated in a proxy (`issue#27221 <http://tracker.ceph.com/issues/27221>`_, `pr#24700 <https://github.com/ceph/ceph/pull/24700>`_, Casey Bodley)
* rgw: Swift SLO size_bytes member is optional (`issue#18936 <http://tracker.ceph.com/issues/18936>`_, `pr#22967 <https://github.com/ceph/ceph/pull/22967>`_, Matt Benjamin)
* rgw: Swift's TempURL can handle temp_url_expires written in ISO8601 (`issue#20795 <http://tracker.ceph.com/issues/20795>`_, `pr#16658 <https://github.com/ceph/ceph/pull/16658>`_, Radoslaw Zarzynski)
* rgw: sync module: avoid printing attrs of objects in log (`issue#37646 <http://tracker.ceph.com/issues/37646>`_, `pr#25541 <https://github.com/ceph/ceph/pull/25541>`_, Abhishek Lekshmanan)
* rgw: test bi list (`issue#24483 <http://tracker.ceph.com/issues/24483>`_, `pr#21772 <https://github.com/ceph/ceph/pull/21772>`_, Orit Wasserman)
* rgw: test/rgw: add ifdef for HAVE_BOOST_CONTEXT (`pr#25744 <https://github.com/ceph/ceph/pull/25744>`_, Casey Bodley)
* rgw,tests: qa: add test for https://github.com/ceph/ceph/pull/22790 (`pr#23143 <https://github.com/ceph/ceph/pull/23143>`_, yuliyang)
* rgw,tests: qa/rgw: add cls_lock/log/refcount/version tests to verify suite (`pr#25381 <https://github.com/ceph/ceph/pull/25381>`_, Casey Bodley)
* rgw,tests: qa/rgw: add missing import line (`pr#25298 <https://github.com/ceph/ceph/pull/25298>`_, Shilpa Jagannath)
* rgw,tests: qa/rgw: add radosgw-admin-rest task to singleton suite (`pr#23145 <https://github.com/ceph/ceph/pull/23145>`_, Casey Bodley)
* rgw,tests: qa/rgw: disable testing on ec-cache pools (`issue#23965 <http://tracker.ceph.com/issues/23965>`_, `pr#22126 <https://github.com/ceph/ceph/pull/22126>`_, Casey Bodley)
* rgw,tests: qa/rgw: fix invalid syntax error in radosgw_admin_rest.py (`issue#37440 <http://tracker.ceph.com/issues/37440>`_, `pr#25305 <https://github.com/ceph/ceph/pull/25305>`_, Casey Bodley)
* rgw,tests: qa/rgw: move ragweed upgrade test into upgrade/luminous-x (`pr#21707 <https://github.com/ceph/ceph/pull/21707>`_, Casey Bodley)
* rgw,tests: qa/rgw: override valgrind --max-threads for radosgw (`issue#25214 <http://tracker.ceph.com/issues/25214>`_, `pr#23372 <https://github.com/ceph/ceph/pull/23372>`_, Casey Bodley)
* rgw,tests: qa/rgw: patch keystone requirements.txt (`issue#23659 <http://tracker.ceph.com/issues/23659>`_, `pr#23402 <https://github.com/ceph/ceph/pull/23402>`_, Casey Bodley)
* rgw,tests: qa/rgw: reduce number of multisite log shards (`pr#24011 <https://github.com/ceph/ceph/pull/24011>`_, Casey Bodley)
* rgw,tests: qa/rgw: reorganize verify tasks (`pr#22249 <https://github.com/ceph/ceph/pull/22249>`_, Casey Bodley)
* rgw,tests: qa/rgw/tempest: either force os_type or select random distro (`pr#25996 <https://github.com/ceph/ceph/pull/25996>`_, Yehuda Sadeh)
* rgw,tests: test/rgw: fix for bucket checkpoints (`issue#24212 <http://tracker.ceph.com/issues/24212>`_, `pr#22124 <https://github.com/ceph/ceph/pull/22124>`_, Casey Bodley)
* rgw,tests: test/rgw: fix race in test_rgw_reshard_wait (`pr#26741 <https://github.com/ceph/ceph/pull/26741>`_, Casey Bodley)
* rgw,tests: test/rgw: silence -Wsign-compare warnings (`pr#26364 <https://github.com/ceph/ceph/pull/26364>`_, Kefu Chai)
* rgw: The delete markers generated by object expiration should have owner attribute (`issue#24568 <http://tracker.ceph.com/issues/24568>`_, `pr#22619 <https://github.com/ceph/ceph/pull/22619>`_, Zhang Shaowen)
* rgw: the error code returned by rgw is different from amz s3 when getting cors (`issue#26964 <http://tracker.ceph.com/issues/26964>`_, `pr#23646 <https://github.com/ceph/ceph/pull/23646>`_, ashitakasam)
* rgw: thread DoutPrefixProvider into RGW::Auth_S3::authorize (`pr#24409 <https://github.com/ceph/ceph/pull/24409>`_, Ali Maredia)
* rgw,tools: ceph-dencoder: add RGWRealm and RGWPeriod  support (`pr#25057 <https://github.com/ceph/ceph/pull/25057>`_, yuliyang)
* rgw,tools: cls: refcount: add obj_refcount to ceph-dencoder (`pr#25441 <https://github.com/ceph/ceph/pull/25441>`_, Abhishek Lekshmanan)
* rgw,tools: cls/rgw: ready rgw_usage_log_entry for extraction via ceph-dencoder (`issue#34537 <http://tracker.ceph.com/issues/34537>`_, `pr#22344 <https://github.com/ceph/ceph/pull/22344>`_, Vaibhav Bhembre)
* rgw,tools: vstart: make beast as the default frontend for rgw (`pr#26566 <https://github.com/ceph/ceph/pull/26566>`_, Abhishek Lekshmanan)
* rgw,tools: vstart: rgw: disable the lc debug interval option (`pr#25487 <https://github.com/ceph/ceph/pull/25487>`_, Abhishek Lekshmanan)
* rgw,tools: vstart: set admin socket for RGW in conf (`pr#23983 <https://github.com/ceph/ceph/pull/23983>`_, Abhishek Lekshmanan)
* rgw: update cls_rgw.cc and cls_rgw_const.h (`pr#24001 <https://github.com/ceph/ceph/pull/24001>`_, yuliyang)
* rgw: update ObjectCacheInfo::time_added on overwrite (`issue#24346 <http://tracker.ceph.com/issues/24346>`_, `pr#22324 <https://github.com/ceph/ceph/pull/22324>`_, Casey Bodley)
* rgw: update --url in usage and doc (`pr#22100 <https://github.com/ceph/ceph/pull/22100>`_, Jos Collin)
* rgw: use chunked encoding to get partial results out faster (`issue#12713 <http://tracker.ceph.com/issues/12713>`_, `pr#23940 <https://github.com/ceph/ceph/pull/23940>`_, Robin H. Johnson)
* rgw: use coarse_real_clock for req_state::time (`pr#21893 <https://github.com/ceph/ceph/pull/21893>`_, Casey Bodley)
* rgw: use DoutPrefixProvider to add more context to log output (`pr#21700 <https://github.com/ceph/ceph/pull/21700>`_, Casey Bodley)
* rgw: use partial-order bucket listing in RGWLC, add configurable processing delay (`issue#23956 <http://tracker.ceph.com/issues/23956>`_, `pr#21755 <https://github.com/ceph/ceph/pull/21755>`_, Matt Benjamin)
* rgw: User Policy (`pr#21379 <https://github.com/ceph/ceph/pull/21379>`_, Pritha Srivastava)
* rgw: user stats account for resharded buckets (`pr#24595 <https://github.com/ceph/ceph/pull/24595>`_, Casey Bodley)
* rgw: warn if zone doesn't contain all zg's placement targets (`pr#22452 <https://github.com/ceph/ceph/pull/22452>`_, Abhishek Lekshmanan)
* rgw: website routing rules num limit (`pr#23429 <https://github.com/ceph/ceph/pull/23429>`_, yuliyang)
* rgw: when exclusive lock fails due existing lock, log add'l info (`issue#38171 <http://tracker.ceph.com/issues/38171>`_, `pr#26272 <https://github.com/ceph/ceph/pull/26272>`_, J. Eric Ivancich)
* rgw: zone service only provides const access to its data (`pr#25412 <https://github.com/ceph/ceph/pull/25412>`_, Casey Bodley)
* rocksdb: pick up a fix to be backward compatible (`issue#25146 <http://tracker.ceph.com/issues/25146>`_, `pr#25070 <https://github.com/ceph/ceph/pull/25070>`_, Kefu Chai)
* script: build-integration-branch: avoid Unicode error (`issue#24003 <http://tracker.ceph.com/issues/24003>`_, `pr#21807 <https://github.com/ceph/ceph/pull/21807>`_, Nathan Cutler)
* script/kubejacker: Add openSUSE based images (`pr#24055 <https://github.com/ceph/ceph/pull/24055>`_, Sebastian Wagner)
* scripts: backport-create-issue: complain about duplicates and support mimic (`issue#24071 <http://tracker.ceph.com/issues/24071>`_, `pr#21634 <https://github.com/ceph/ceph/pull/21634>`_, Nathan Cutler)
* seastar: pickup fix for segfault in POSIX stack (`pr#25861 <https://github.com/ceph/ceph/pull/25861>`_, Kefu Chai)
* spec: add missing rbd mirror bootstrap directory (`pr#24856 <https://github.com/ceph/ceph/pull/24856>`_, Sébastien Han)
* src: balance std::hex and std::dec manipulators (`pr#22287 <https://github.com/ceph/ceph/pull/22287>`_, Kefu Chai)
* src/ceph.in: dev mode: add build path to beginning of PATH, not end (`issue#24578 <http://tracker.ceph.com/issues/24578>`_, `pr#22628 <https://github.com/ceph/ceph/pull/22628>`_, Dan Mick)
* src: Eliminate new warnings in Fedora 28 (`pr#21898 <https://github.com/ceph/ceph/pull/21898>`_, Adam C. Emerson)
* test/crimson: fixes of unittest_seastar_echo (`pr#26419 <https://github.com/ceph/ceph/pull/26419>`_, Yingxin Cheng, Kefu Chai)
* test/fio: fix compiler failure (`pr#22728 <https://github.com/ceph/ceph/pull/22728>`_, Jianpeng Ma)
* test/fio: new option to control file preallocation (`pr#23410 <https://github.com/ceph/ceph/pull/23410>`_, Igor Fedotov)
* tests: Add hashinfo testing for dump command of ceph-objectstore-tool (`issue#38053 <http://tracker.ceph.com/issues/38053>`_, `pr#26158 <https://github.com/ceph/ceph/pull/26158>`_, David Zafman)
* tests: add ubuntu 18.04 dockerfile (`pr#25251 <https://github.com/ceph/ceph/pull/25251>`_, Kefu Chai)
* tests: auth, test: fix building on ARMs after the NSS -> OpenSSL transition (`pr#22129 <https://github.com/ceph/ceph/pull/22129>`_, Radoslaw Zarzynski)
* tests: ceph_kvstorebench: include <errno.h> not asm-generic/errno.h (`pr#25256 <https://github.com/ceph/ceph/pull/25256>`_, Kefu Chai)
* tests: ceph-volume: functional tests, add libvirt customization (`pr#25895 <https://github.com/ceph/ceph/pull/25895>`_, Jan Fajerski)
* tests: do not check for invalid k/m combinations (`issue#16500 <http://tracker.ceph.com/issues/16500>`_, `pr#25046 <https://github.com/ceph/ceph/pull/25046>`_, Kefu Chai)
* tests: Fixes for standalone tests (`pr#22480 <https://github.com/ceph/ceph/pull/22480>`_, David Zafman)
* tests: fix to check server_conn in MessengerTest.NameAddrTest (`pr#23931 <https://github.com/ceph/ceph/pull/23931>`_, Yingxin)
* tests: make ceph-admin-commands.sh log what it does (`issue#37089 <http://tracker.ceph.com/issues/37089>`_, `pr#25080 <https://github.com/ceph/ceph/pull/25080>`_, Nathan Cutler)
* tests: make test_ceph_argparse.py pass on py3-only systems (`issue#24816 <http://tracker.ceph.com/issues/24816>`_, `pr#22922 <https://github.com/ceph/ceph/pull/22922>`_, Nathan Cutler)
* tests: mgr/ansible: add install tox==2.9.1 (`pr#26313 <https://github.com/ceph/ceph/pull/26313>`_, Kefu Chai)
* tests: mgr/dashboard: Added additional breadcrumb and tab tests to Cluster menu (`pr#26151 <https://github.com/ceph/ceph/pull/26151>`_, Nathan Weinberg)
* tests: mgr/dashboard: Added additional breadcrumb tests to Cluster (`pr#25010 <https://github.com/ceph/ceph/pull/25010>`_, Nathan Weinberg)
* tests: mgr/dashboard: Added breadcrumb and tab tests to Pools menu (`pr#25572 <https://github.com/ceph/ceph/pull/25572>`_, Nathan Weinberg)
* tests: mgr/dashboard: Added breadcrumb tests to Block menu items (`pr#25143 <https://github.com/ceph/ceph/pull/25143>`_, Nathan Weinberg)
* tests: mgr/dashboard: Added breadcrumb tests to Filesystems menu (`pr#26592 <https://github.com/ceph/ceph/pull/26592>`_, Nathan Weinberg)
* tests: mgr/dashboard: Added NFS Ganesha suite to QA tests (`pr#26510 <https://github.com/ceph/ceph/pull/26510>`_, Laura Paduano)
* tests: mgr/dashboard: Added tab tests to Block menu items (`pr#26243 <https://github.com/ceph/ceph/pull/26243>`_, Nathan Weinberg)
* tests: mgr/dashboard: Add Jest Runner (`pr#22031 <https://github.com/ceph/ceph/pull/22031>`_, Tiago Melo)
* tests: mgr/dashboard: Add unit test case for controller/erasure_code_profile.py (`pr#24789 <https://github.com/ceph/ceph/pull/24789>`_, Ranjitha G)
* tests: mgr/dashboard: Add unit test for frontend api services (`pr#22284 <https://github.com/ceph/ceph/pull/22284>`_, Tiago Melo)
* tests: mgr/dashboard: Add unit tests for all frontend pipes (`pr#22182 <https://github.com/ceph/ceph/pull/22182>`_, Tiago Melo)
* tests: mgr/dashboard: Add unit test to the frontend services (`pr#22244 <https://github.com/ceph/ceph/pull/22244>`_, Tiago Melo)
* tests: mgr/dashboard: Fix a broken ECP controller test (`pr#25363 <https://github.com/ceph/ceph/pull/25363>`_, Zack Cerza)
* tests: mgr/dashboard: Fix PYTHONPATH for test runner (`pr#25359 <https://github.com/ceph/ceph/pull/25359>`_, Zack Cerza)
* tests: mgr/dashboard: Improve max-line-length tslint rule (`pr#22279 <https://github.com/ceph/ceph/pull/22279>`_, Tiago Melo)
* tests: mgr/dashboard: RbdMirroringService test suite fails in dev mode (`issue#37841 <http://tracker.ceph.com/issues/37841>`_, `pr#25865 <https://github.com/ceph/ceph/pull/25865>`_, Stephan Müller)
* tests: mgr/dashboard: Small improvements for running teuthology tests (`pr#25121 <https://github.com/ceph/ceph/pull/25121>`_, Zack Cerza)
* tests: mgr/dashboard: updated API test (`pr#25653 <https://github.com/ceph/ceph/pull/25653>`_, Alfonso Martínez)
* tests: mgr/dashboard: updated API test to reflect changes in ModuleInfo (`pr#25761 <https://github.com/ceph/ceph/pull/25761>`_, Kefu Chai)
* tests: mgr/test_orchestrator: correct ceph-volume path (`issue#37773 <http://tracker.ceph.com/issues/37773>`_, `pr#25839 <https://github.com/ceph/ceph/pull/25839>`_, Kefu Chai)
* tests: object errors found in be_select_auth_object() aren't logged the same (`issue#25108 <http://tracker.ceph.com/issues/25108>`_, `pr#23376 <https://github.com/ceph/ceph/pull/23376>`_, David Zafman)
* tests: osd/OSDMap: set pg_autoscale_mode with setting from conf (`pr#25746 <https://github.com/ceph/ceph/pull/25746>`_, Kefu Chai)
* tests: os/tests: fix garbageCollection test case from store_test suite (`pr#23752 <https://github.com/ceph/ceph/pull/23752>`_, Igor Fedotov)
* tests: os/tests: silence -Wsign-compare warning (`pr#25072 <https://github.com/ceph/ceph/pull/25072>`_, Kefu Chai)
* tests: qa: add librados3 to exclude_packages for ugprade tests (`pr#25037 <https://github.com/ceph/ceph/pull/25037>`_, Kefu Chai)
* tests: qa: add test that builds example librados programs (`issue#35989 <http://tracker.ceph.com/issues/35989>`_, `issue#15100 <http://tracker.ceph.com/issues/15100>`_, `pr#23131 <https://github.com/ceph/ceph/pull/23131>`_, Nathan Cutler)
* tests: qa/ceph-ansible: Set ceph_stable_release to mimic (`issue#38231 <http://tracker.ceph.com/issues/38231>`_, `pr#26328 <https://github.com/ceph/ceph/pull/26328>`_, Brad Hubbard)
* tests: qa/distros: add openSUSE Leap 42.3 and 15.0 (`pr#24380 <https://github.com/ceph/ceph/pull/24380>`_, Nathan Cutler)
* tests: qa: Don't use sudo when moving logs (`pr#22763 <https://github.com/ceph/ceph/pull/22763>`_, David Zafman)
* tests: qa: downgrade librados2,librbd1 for thrash-old-clients tests (`issue#37618 <http://tracker.ceph.com/issues/37618>`_, `pr#25463 <https://github.com/ceph/ceph/pull/25463>`_, Kefu Chai)
* tests: qa: fix manager module paths (`pr#23637 <https://github.com/ceph/ceph/pull/23637>`_, Noah Watkins, David Zafman)
* tests/qa - fix mimic subset for nightlies (`pr#21931 <https://github.com/ceph/ceph/pull/21931>`_, Yuri Weinstein)
* tests: qa: fix test on "ceph fs set cephfs allow_new_snaps" (`pr#21829 <https://github.com/ceph/ceph/pull/21829>`_, Kefu Chai)
* tests: qa: fix upgrade tests and test_envlibrados_for_rocksdb.sh (`pr#25106 <https://github.com/ceph/ceph/pull/25106>`_, Kefu Chai)
* tests: qa: For teuthology copy logs to teuthology expected location (`pr#22702 <https://github.com/ceph/ceph/pull/22702>`_, David Zafman)
* tests: qa/mgr/dashboard: Fix type annotation error (`pr#25235 <https://github.com/ceph/ceph/pull/25235>`_, Sebastian Wagner)
* tests: qa/mon: fix cluster support for monmap bootstrap (`issue#38115 <http://tracker.ceph.com/issues/38115>`_, `pr#26205 <https://github.com/ceph/ceph/pull/26205>`_, Casey Bodley)
* tests: qa/standalone: Minor test improvements (`issue#35912 <http://tracker.ceph.com/issues/35912>`_, `pr#24018 <https://github.com/ceph/ceph/pull/24018>`_, David Zafman)
* tests: qa/standalone/scrub: When possible show side-by-side diff in addition to regular diff (`pr#22727 <https://github.com/ceph/ceph/pull/22727>`_, David Zafman)
* tests:  qa/standalone: Standalone test corrections (`issue#35982 <http://tracker.ceph.com/issues/35982>`_, `pr#24088 <https://github.com/ceph/ceph/pull/24088>`_, David Zafman)
* tests: qa/suites/rados/upgrade: remove stray link (`pr#22460 <https://github.com/ceph/ceph/pull/22460>`_, Sage Weil)
* tests: qa/suites/rados/upgrade: set require-osd-release to nautilus (`issue#37432 <http://tracker.ceph.com/issues/37432>`_, `pr#25314 <https://github.com/ceph/ceph/pull/25314>`_, Kefu Chai)
* tests: qa/suites/rados/verify: remove random-distro$ (`pr#22057 <https://github.com/ceph/ceph/pull/22057>`_, Kefu Chai)
* tests: qa/suites/upgrade/mimic-x: fix rhel runs (`pr#25781 <https://github.com/ceph/ceph/pull/25781>`_, Neha Ojha)
* tests: qa/tasks/mgr: fix test_pool.py (`issue#24077 <http://tracker.ceph.com/issues/24077>`_, `pr#21943 <https://github.com/ceph/ceph/pull/21943>`_, Kefu Chai)
* tests: qa/tasks/thrashosds-health.yaml: whitelist slow requests (`issue#25104 <http://tracker.ceph.com/issues/25104>`_, `pr#23237 <https://github.com/ceph/ceph/pull/23237>`_, Neha Ojha)
* tests: qa/tasks: update mirror link for maven (`pr#23944 <https://github.com/ceph/ceph/pull/23944>`_, Vasu Kulkarni)
* tests: qa/tests: added filters to support distro tests for client-upgrade tests (`pr#22096 <https://github.com/ceph/ceph/pull/22096>`_, Yuri Weinstein)
* tests: qa/tests - added mimic-p2p suite (`pr#22726 <https://github.com/ceph/ceph/pull/22726>`_, Yuri Weinstein)
* tests: qa/tests: Added mimic runs, removed large suites (rados, rbd, etc) ru… (`pr#21827 <https://github.com/ceph/ceph/pull/21827>`_, Yuri Weinstein)
* tests: qa/tests: added "-n 7" to make sure mimic-x runs on built master branch (`pr#25038 <https://github.com/ceph/ceph/pull/25038>`_, Yuri Weinstein)
* tests: qa/tests: added rhel 7.6 (`pr#25919 <https://github.com/ceph/ceph/pull/25919>`_, Yuri Weinstein)
* tests: qa/tests: fix volume size when running in ovh (`pr#21961 <https://github.com/ceph/ceph/pull/21961>`_, Vasu Kulkarni)
* tests: qa/tests: Move ceph-ansible tests to ansible version 2.7 (`issue#37973 <http://tracker.ceph.com/issues/37973>`_, `pr#26068 <https://github.com/ceph/ceph/pull/26068>`_, Brad Hubbard)
* tests: qa/tests: remove ceph-disk tests from ceph-deploy and default all tests to use ceph-volume (`pr#22921 <https://github.com/ceph/ceph/pull/22921>`_, Vasu Kulkarni)
* tests: qa/upgrade: cleanup for nautilus (`pr#23305 <https://github.com/ceph/ceph/pull/23305>`_, Nathan Cutler)
* tests: qa: use $TESTDIR for testing mkfs (`pr#22246 <https://github.com/ceph/ceph/pull/22246>`_, Kefu Chai)
* tests: qa: wait longer for osd to flush pg stats (`issue#24321 <http://tracker.ceph.com/issues/24321>`_, `pr#22275 <https://github.com/ceph/ceph/pull/22275>`_, Kefu Chai)
* tests: qa/workunits/ceph-disk: --no-mon-config (`pr#21942 <https://github.com/ceph/ceph/pull/21942>`_, Kefu Chai)
* tests: qa/workunits/mon/test_mon_config_key.py: bump up the size limit (`issue#36260 <http://tracker.ceph.com/issues/36260>`_, `pr#24340 <https://github.com/ceph/ceph/pull/24340>`_, Kefu Chai)
* tests: qa/workunits/rados/test_envlibrados_for_rocksdb: install g++ not g++-4.7 (`pr#22103 <https://github.com/ceph/ceph/pull/22103>`_, Kefu Chai)
* tests: qa/workunits/rados/test_librados_build.sh: grab files from explicit git branch (`pr#25268 <https://github.com/ceph/ceph/pull/25268>`_, Nathan Cutler)
* tests: run-make-check: increase fs.aio-max-nr to 1048576 (`pr#23689 <https://github.com/ceph/ceph/pull/23689>`_, Kefu Chai)
* tests: test,common: silence GCC warnings (`pr#23692 <https://github.com/ceph/ceph/pull/23692>`_, Kefu Chai)
* tests: test/crimson: add dummy_auth to test_async_echo (`pr#26783 <https://github.com/ceph/ceph/pull/26783>`_, Yingxin Cheng)
* tests: test/crimson: fix build failure of test_alien_echo (`pr#26308 <https://github.com/ceph/ceph/pull/26308>`_, chunmei Liu)
* tests: test/crimson: fix FTBFS of unittest_seastar_perfcounters on arm64 (`pr#25647 <https://github.com/ceph/ceph/pull/25647>`_, Kefu Chai)
* tests: test/crimson: split async-msgr out of alien_echo (`pr#26620 <https://github.com/ceph/ceph/pull/26620>`_, Yingxin Cheng)
* tests: test/dashboard: fix segfault when importing dm.xmlsec.binding (`issue#37081 <http://tracker.ceph.com/issues/37081>`_, `pr#25139 <https://github.com/ceph/ceph/pull/25139>`_, Kefu Chai)
* tests: test: Disable duplicate request command test during scrub testing (`pr#25675 <https://github.com/ceph/ceph/pull/25675>`_, David Zafman)
* tests: test/docker-test-helper.sh: move "cp .git/HEAD" out of loop (`pr#22978 <https://github.com/ceph/ceph/pull/22978>`_, Kefu Chai)
* tests: test/encoding: Fix typo in encoding/types.h file (`pr#22332 <https://github.com/ceph/ceph/pull/22332>`_, TommyLike)
* tests: test/fio:  pass config params to object store in a different manner (`pr#23267 <https://github.com/ceph/ceph/pull/23267>`_, Igor Fedotov)
* tests: test: fix compile error in test/crimson/test_config.cc (`pr#23724 <https://github.com/ceph/ceph/pull/23724>`_, Yingxin)
* tests: test: fix libc++ crash in Log.GarbleRecovery (`pr#25135 <https://github.com/ceph/ceph/pull/25135>`_, Casey Bodley)
* tests: test/librados: fix LibRadosList.ListObjectsNS (`pr#22771 <https://github.com/ceph/ceph/pull/22771>`_, Kefu Chai)
* tests: test: Limit loops waiting for force-backfill/force-recovery to happen (`issue#38309 <http://tracker.ceph.com/issues/38309>`_, `pr#26416 <https://github.com/ceph/ceph/pull/26416>`_, David Zafman)
* tests: test: Need to escape parens in log-whitelist for grep (`pr#22074 <https://github.com/ceph/ceph/pull/22074>`_, David Zafman)
* tests: test: osd-backfill-stats.sh Fix check of multi backfill OSDs, skip re… (`pr#26330 <https://github.com/ceph/ceph/pull/26330>`_, David Zafman)
* tests: test/pybind/test_rados.py: collect output in stdout for "bench" cmd (`pr#21957 <https://github.com/ceph/ceph/pull/21957>`_, Kefu Chai)
* tests: test: run-standalone.sh: point LD_LIBRARY_PATH to $(pwd)/lib (`issue#38262 <http://tracker.ceph.com/issues/38262>`_, `pr#26371 <https://github.com/ceph/ceph/pull/26371>`_, David Zafman)
* tests: tests/qa: trying $ distro mix (`pr#21895 <https://github.com/ceph/ceph/pull/21895>`_, Yuri Weinstein)
* tests: test: Start using GNU awk and fix archiving directory (`pr#23955 <https://github.com/ceph/ceph/pull/23955>`_, Willem Jan Withagen)
* tests: test/strtol: add test case for parsing hex numbers (`pr#21582 <https://github.com/ceph/ceph/pull/21582>`_, Jan Fajerski)
* tests: test: suppress core dumping in there tests as well (`pr#25311 <https://github.com/ceph/ceph/pull/25311>`_, Willem Jan Withagen)
* tests: test: switch to GNU sed on FreeBSD (`pr#26318 <https://github.com/ceph/ceph/pull/26318>`_, Willem Jan Withagen)
* tests: test: test_get_timeout_delays() fix (`pr#22837 <https://github.com/ceph/ceph/pull/22837>`_, David Zafman)
* tests: test: Use a file that should be on all OSes (`pr#22428 <https://github.com/ceph/ceph/pull/22428>`_, David Zafman)
* tests: test: Use a grep pattern that works across releases (`issue#35845 <http://tracker.ceph.com/issues/35845>`_, `pr#24013 <https://github.com/ceph/ceph/pull/24013>`_, David Zafman)
* tests: test: Use pids instead of jobspecs which were wrong (`issue#27056 <http://tracker.ceph.com/issues/27056>`_, `pr#23695 <https://github.com/ceph/ceph/pull/23695>`_, David Zafman)
* tests: test: wait_for_pg_stats() should do another check after last 13 secon… (`pr#22198 <https://github.com/ceph/ceph/pull/22198>`_, David Zafman)
* tests: test: Whitelist corrections (`pr#22164 <https://github.com/ceph/ceph/pull/22164>`_, David Zafman)
* tests: test: write log file to current directory (`issue#36737 <http://tracker.ceph.com/issues/36737>`_, `pr#25704 <https://github.com/ceph/ceph/pull/25704>`_, Kefu Chai)
* tests,tools: ceph-objectstore-tool: Dump hashinfo (`issue#37597 <http://tracker.ceph.com/issues/37597>`_, `pr#25483 <https://github.com/ceph/ceph/pull/25483>`_, David Zafman)
* tests: update Dockerfile to support fc-29 (`pr#26311 <https://github.com/ceph/ceph/pull/26311>`_, Kefu Chai)
* tests: upgrade/luminous-x: fix order of final-workload directory (`pr#23162 <https://github.com/ceph/ceph/pull/23162>`_, Nathan Cutler)
* tests: upgrade/luminous-x: whitelist REQUEST_SLOW for rados_mon_thrash (`issue#25051 <http://tracker.ceph.com/issues/25051>`_, `pr#23160 <https://github.com/ceph/ceph/pull/23160>`_, Nathan Cutler)
* tests: Wip 38027 38195: osd/osd-backfill-space.sh fails (`issue#38027 <http://tracker.ceph.com/issues/38027>`_, `issue#38195 <http://tracker.ceph.com/issues/38195>`_, `pr#26290 <https://github.com/ceph/ceph/pull/26290>`_, David Zafman)
* tools: Add clear-data-digest command to objectstore tool (`pr#25403 <https://github.com/ceph/ceph/pull/25403>`_, Li Yichao)
* tools: add offset-align option to "rados" load-gen (`pr#20683 <https://github.com/ceph/ceph/pull/20683>`_, Zengran Zhang)
* tools: backport-create-issue: rate-limit to avoid seeming like a spammer (`pr#24243 <https://github.com/ceph/ceph/pull/24243>`_, Nathan Cutler)
* tools: ceph-menv: mrun shell environment (`pr#22132 <https://github.com/ceph/ceph/pull/22132>`_, Yehuda Sadeh)
* tools: ceph-objectstore-tool: Allow target level as first positional argument (`issue#35846 <http://tracker.ceph.com/issues/35846>`_, `pr#23989 <https://github.com/ceph/ceph/pull/23989>`_, David Zafman)
* tools: correct the description of Allowed options in osdomap tool (`pr#23488 <https://github.com/ceph/ceph/pull/23488>`_, xiaomanh)
* tools, mgr: silence clang warnings (`pr#23430 <https://github.com/ceph/ceph/pull/23430>`_, Kefu Chai)
* tools: mstop.sh allow kill -9 after failing to kill procs (`pr#26680 <https://github.com/ceph/ceph/pull/26680>`_, Yuval Lifshitz)
* tools/rados: fix memory leak in error path (`pr#25410 <https://github.com/ceph/ceph/pull/25410>`_, Li Wang)
* tools: script/kubejacker: include cls libs (`pr#23569 <https://github.com/ceph/ceph/pull/23569>`_, John Spray)
* tools: script: new ceph-backport.sh script (`pr#22875 <https://github.com/ceph/ceph/pull/22875>`_, Nathan Cutler)
* tools:  tools: ceph-authtool: report correct number of caps when creating keyring (`pr#23304 <https://github.com/ceph/ceph/pull/23304>`_, Nathan Cutler)
* tools: tools/ceph_kvstore_tool: do not open rocksdb when repairing it (`pr#25108 <https://github.com/ceph/ceph/pull/25108>`_, Kefu Chai)
* tools: tools/ceph_kvstore_tool: extract StoreTool into kvstore_tool.cc (`pr#26041 <https://github.com/ceph/ceph/pull/26041>`_, Kefu Chai)
* tools: tools/ceph_kvstore_tool: Move summary output to print_summary (`pr#26666 <https://github.com/ceph/ceph/pull/26666>`_, Brad Hubbard)
* tools: tools/rados: allow list objects in a specific pg in a pool (`pr#19041 <https://github.com/ceph/ceph/pull/19041>`_, Li Wang)
* tools: tools/rados: always call rados.shutdown() before exit() (`issue#36732 <http://tracker.ceph.com/issues/36732>`_, `pr#24990 <https://github.com/ceph/ceph/pull/24990>`_, Li Wang)
* tools: tools/rados: correct the read offset of bench (`pr#23667 <https://github.com/ceph/ceph/pull/23667>`_, Xiaofei Cui)
* tools: tools/rados: fix the unit of target-throughput (`pr#23683 <https://github.com/ceph/ceph/pull/23683>`_, Xiaofei Cui)
* vstart: disable dashboard when rbd not built (`pr#23336 <https://github.com/ceph/ceph/pull/23336>`_, Noah Watkins)
* vstart.sh: fix params generation for monmaptool (`issue#38174 <http://tracker.ceph.com/issues/38174>`_, `pr#26273 <https://github.com/ceph/ceph/pull/26273>`_, Yehuda Sadeh)
