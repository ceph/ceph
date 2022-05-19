=======
Pacific
=======

v16.2.9 Pacific
===============

This is a hotfix release in the Pacific series to address a bug in 16.2.8 that could cause MGRs to deadlock. See https://tracker.ceph.com/issues/55687.

Changelog
---------

* mgr/ActivePyModules.cc: fix cases where GIL is held while attempting to lock mutex (`pr#46302 <https://github.com/ceph/ceph/pull/46302>`_, Cory Snyder)

v16.2.8 Pacific   
===============

This is the eighth backport release in the Pacific series.

Notable Changes
---------------

* MON/MGR: Pools can now be created with `--bulk` flag. Any pools created with `bulk`
  will use a profile of the `pg_autoscaler` that provides more performance from the start.
  However, any pools created without the `--bulk` flag will remain using it's old behavior
  by default. For more details, see:

  https://docs.ceph.com/en/latest/rados/operations/placement-groups/

* MGR: The pg_autoscaler can now be turned `on` and `off` globally
  with the `noautoscale` flag. By default this flag is unset and
  the default pg_autoscale mode remains the same.
  For more details, see:

  https://docs.ceph.com/en/latest/rados/operations/placement-groups/

* A health warning will now be reported if the ``require-osd-release`` flag is not
  set to the appropriate release after a cluster upgrade.

* CephFS: Upgrading Ceph Metadata Servers when using multiple active MDSs requires
  ensuring no pending stray entries which are directories are present for active
  ranks except rank 0. See :ref:`upgrading_from_octopus_or_nautilus`.

Changelog
---------

* [Revert] bluestore: set upper and lower bounds on rocksdb omap iterators (`pr#46092 <https://github.com/ceph/ceph/pull/46092>`_, Neha Ojha)
* admin/doc-requirements: bump sphinx to 4.4.0 (`pr#45876 <https://github.com/ceph/ceph/pull/45876>`_, Kefu Chai)
* auth,mon: don't log "unable to find a keyring" error when key is given (`pr#43313 <https://github.com/ceph/ceph/pull/43313>`_, Ilya Dryomov)
* backport nbd cookie support (`pr#45582 <https://github.com/ceph/ceph/pull/45582>`_, Prasanna Kumar Kalever)
* backport of monitoring related PRs (`pr#45980 <https://github.com/ceph/ceph/pull/45980>`_, Pere Diaz Bou, Travis Nielsen, Aashish Sharma, Nizamudeen A, Arthur Outhenin-Chalandre)
* bluestore: set upper and lower bounds on rocksdb omap iterators (`pr#45963 <https://github.com/ceph/ceph/pull/45963>`_, Cory Snyder)
* build: Add some debugging messages (`pr#45753 <https://github.com/ceph/ceph/pull/45753>`_, David Galloway)
* build: install-deps failing in docker build (`pr#45849 <https://github.com/ceph/ceph/pull/45849>`_, Nizamudeen A, Ernesto Puerta)
* ceph-fuse: perform cleanup if test_dentry_handling failed (`pr#45351 <https://github.com/ceph/ceph/pull/45351>`_, Nikhilkumar Shelke)
* ceph-volume: abort when passed devices have partitions (`pr#45146 <https://github.com/ceph/ceph/pull/45146>`_, Guillaume Abrioux)
* ceph-volume: don't use MultiLogger in find_executable_on_host() (`pr#44701 <https://github.com/ceph/ceph/pull/44701>`_, Guillaume Abrioux)
* ceph-volume: fix error 'KeyError' with inventory (`pr#44884 <https://github.com/ceph/ceph/pull/44884>`_, Guillaume Abrioux)
* ceph-volume: fix regression introcuded via #43536 (`pr#44644 <https://github.com/ceph/ceph/pull/44644>`_, Guillaume Abrioux)
* ceph-volume: fix tags dict output in `lvm list` (`pr#44767 <https://github.com/ceph/ceph/pull/44767>`_, Guillaume Abrioux)
* ceph-volume: honour osd_dmcrypt_key_size option (`pr#44973 <https://github.com/ceph/ceph/pull/44973>`_, Guillaume Abrioux)
* ceph-volume: human_readable_size() refactor (`pr#44209 <https://github.com/ceph/ceph/pull/44209>`_, Guillaume Abrioux)
* ceph-volume: improve mpath devices support (`pr#44789 <https://github.com/ceph/ceph/pull/44789>`_, Guillaume Abrioux)
* ceph-volume: make it possible to skip needs_root() (`pr#44319 <https://github.com/ceph/ceph/pull/44319>`_, Guillaume Abrioux)
* ceph-volume: show RBD devices as not available (`pr#44708 <https://github.com/ceph/ceph/pull/44708>`_, Michael Fritch)
* ceph/admin: s/master/main (`pr#45596 <https://github.com/ceph/ceph/pull/45596>`_, Zac Dover)
* Cephadm Pacific Batch Backport April (`pr#45919 <https://github.com/ceph/ceph/pull/45919>`_, Adam King, Teoman ONAY, Redouane Kachach, Lukas Mayer, Melissa Li)
* Cephadm Pacific Batch Backport March (`pr#45716 <https://github.com/ceph/ceph/pull/45716>`_, Adam King, Redouane Kachach, Matan Breizman, wangyunqing)
* cephadm/ceph-volume: do not use lvm binary in containers (`pr#43954 <https://github.com/ceph/ceph/pull/43954>`_, Guillaume Abrioux, Sage Weil)
* cephadm: _parse_ipv6_route: Fix parsing ifs w/o route (`pr#44877 <https://github.com/ceph/ceph/pull/44877>`_, Sebastian Wagner)
* cephadm: add shared_ceph_folder opt to ceph-volume subcommand (`pr#44880 <https://github.com/ceph/ceph/pull/44880>`_, Guillaume Abrioux)
* cephadm: check if cephadm is root after cli is parsed (`pr#44634 <https://github.com/ceph/ceph/pull/44634>`_, John Mulligan)
* cephadm: chown the prometheus data dir during redeploy (`pr#45046 <https://github.com/ceph/ceph/pull/45046>`_, Michael Fritch)
* cephadm: deal with ambiguity within normalize_image_digest (`pr#44632 <https://github.com/ceph/ceph/pull/44632>`_, Sebastian Wagner)
* cephadm: fix broken telemetry documentation link (`pr#45803 <https://github.com/ceph/ceph/pull/45803>`_, Laura Flores)
* cephadm: infer the default container image during pull (`pr#45569 <https://github.com/ceph/ceph/pull/45569>`_, Michael Fritch)
* cephadm: make extract_uid_gid errors more readable (`pr#44528 <https://github.com/ceph/ceph/pull/44528>`_, Sebastian Wagner)
* cephadm: November batch 2 (`pr#44446 <https://github.com/ceph/ceph/pull/44446>`_, Sage Weil, Adam King, Sebastian Wagner, Melissa Li, Michael Fritch, Guillaume Abrioux)
* cephadm: pass `CEPH_VOLUME_SKIP_RESTORECON=yes` (backport) (`pr#44248 <https://github.com/ceph/ceph/pull/44248>`_, Guillaume Abrioux)
* cephadm: preserve `authorized_keys` file during upgrade (`pr#45355 <https://github.com/ceph/ceph/pull/45355>`_, Michael Fritch)
* cephadm: Remove containers pids-limit (`pr#45580 <https://github.com/ceph/ceph/pull/45580>`_, Ilya Dryomov, Teoman ONAY)
* cephadm: revert pids limit (`pr#45936 <https://github.com/ceph/ceph/pull/45936>`_, Adam King)
* cephadm: validate that the constructed YumDnf baseurl is usable (`pr#44882 <https://github.com/ceph/ceph/pull/44882>`_, John Mulligan)
* cls/journal: skip disconnected clients when calculating min_commit_position (`pr#44690 <https://github.com/ceph/ceph/pull/44690>`_, Mykola Golub)
* cls/rbd: GroupSnapshotNamespace comparator violates ordering rules (`pr#45075 <https://github.com/ceph/ceph/pull/45075>`_, Ilya Dryomov)
* cmake/modules: always use the python3 specified in command line (`pr#45967 <https://github.com/ceph/ceph/pull/45967>`_, Kefu Chai)
* cmake: pass RTE_DEVEL_BUILD=n when building dpdk (`pr#45262 <https://github.com/ceph/ceph/pull/45262>`_, Kefu Chai)
* common/PriorityCache: low perf counters priorities for submodules (`pr#44175 <https://github.com/ceph/ceph/pull/44175>`_, Igor Fedotov)
* common: avoid pthread_mutex_unlock twice (`pr#45464 <https://github.com/ceph/ceph/pull/45464>`_, Dai Zhiwei)
* common: fix FTBFS due to dout & need_dynamic on GCC-12 (`pr#45373 <https://github.com/ceph/ceph/pull/45373>`_, Radoslaw Zarzynski)
* common: fix missing name in PriorityCache perf counters (`pr#45588 <https://github.com/ceph/ceph/pull/45588>`_, Laura Flores)
* common: replace BitVector::NoInitAllocator with wrapper struct (`pr#45179 <https://github.com/ceph/ceph/pull/45179>`_, Casey Bodley)
* crush: Fix segfault in update_from_hook (`pr#44897 <https://github.com/ceph/ceph/pull/44897>`_, Adam Kupczyk)
* doc/cephadm: Add CentOS Stream install instructions (`pr#44996 <https://github.com/ceph/ceph/pull/44996>`_, Patrick C. F. Ernzer)
* doc/cephadm: Co-location of daemons (`pr#44879 <https://github.com/ceph/ceph/pull/44879>`_, Sebastian Wagner)
* doc/cephadm: Doc backport (`pr#44525 <https://github.com/ceph/ceph/pull/44525>`_, Foad Lind, Sebastian Wagner)
* doc/cephadm: improve the development doc a bit (`pr#44636 <https://github.com/ceph/ceph/pull/44636>`_, Radoslaw Zarzynski)
* doc/cephadm: remove duplicate deployment scenario section (`pr#44660 <https://github.com/ceph/ceph/pull/44660>`_, Melissa Li)
* doc/dev: s/repostory/repository/ (really) (`pr#45789 <https://github.com/ceph/ceph/pull/45789>`_, Zac Dover)
* doc/start: add testing support information (`pr#45989 <https://github.com/ceph/ceph/pull/45989>`_, Zac Dover)
* doc/start: include A. D'Atri's hardware-recs recs (`pr#45298 <https://github.com/ceph/ceph/pull/45298>`_, Zac Dover)
* doc/start: remove journal info from hardware recs (`pr#45123 <https://github.com/ceph/ceph/pull/45123>`_, Zac Dover)
* doc/start: remove osd stub from hardware recs (`pr#45316 <https://github.com/ceph/ceph/pull/45316>`_, Zac Dover)
* doc: prerequisites fix for cephFS mount (`pr#44272 <https://github.com/ceph/ceph/pull/44272>`_, Nikhilkumar Shelke)
* doc: Use older mistune (`pr#44226 <https://github.com/ceph/ceph/pull/44226>`_, David Galloway)
* Enable autotune for osd_memory_target on bootstrap (`pr#44633 <https://github.com/ceph/ceph/pull/44633>`_, Melissa Li)
* krbd: return error when no initial monitor address found (`pr#45003 <https://github.com/ceph/ceph/pull/45003>`_, Burt Holzman)
* librados: check latest osdmap on ENOENT in pool_reverse_lookup() (`pr#45586 <https://github.com/ceph/ceph/pull/45586>`_, Ilya Dryomov)
* librbd/cache/pwl: misc backports (`pr#44199 <https://github.com/ceph/ceph/pull/44199>`_, Jianpeng Ma, Jason Dillaman)
* librbd: diff-iterate reports incorrect offsets in fast-diff mode (`pr#44547 <https://github.com/ceph/ceph/pull/44547>`_, Ilya Dryomov)
* librbd: fix use-after-free on ictx in list_descendants() (`pr#44999 <https://github.com/ceph/ceph/pull/44999>`_, Ilya Dryomov, Wang ShuaiChao)
* librbd: fix various memory leaks (`pr#44998 <https://github.com/ceph/ceph/pull/44998>`_, Or Ozeri)
* librbd: make diff-iterate in fast-diff mode sort and merge reported extents (`pr#45638 <https://github.com/ceph/ceph/pull/45638>`_, Ilya Dryomov)
* librbd: readv/writev fix iovecs length computation overflow (`pr#45561 <https://github.com/ceph/ceph/pull/45561>`_, Jonas Pfefferle)
* librbd: restore diff-iterate include_parent functionality in fast-diff mode (`pr#44594 <https://github.com/ceph/ceph/pull/44594>`_, Ilya Dryomov)
* librgw: make rgw file handle versioned (`pr#45495 <https://github.com/ceph/ceph/pull/45495>`_, Xuehan Xu)
* librgw: treat empty root path as "/" on mount (`pr#43968 <https://github.com/ceph/ceph/pull/43968>`_, Matt Benjamin)
* mds,client: add new getvxattr op (`pr#45487 <https://github.com/ceph/ceph/pull/45487>`_, Milind Changire)
* mds: add mds_dir_max_entries config option (`pr#44512 <https://github.com/ceph/ceph/pull/44512>`_, Yongseok Oh)
* mds: directly return just after responding the link request (`pr#44620 <https://github.com/ceph/ceph/pull/44620>`_, Xiubo Li)
* mds: dump tree '/' when the path is empty (`pr#44622 <https://github.com/ceph/ceph/pull/44622>`_, Xiubo Li)
* mds: ensure that we send the btime in cap messages (`pr#45163 <https://github.com/ceph/ceph/pull/45163>`_, Jeff Layton)
* mds: fails to reintegrate strays if destdn's directory is full (ENOSPC) (`pr#44513 <https://github.com/ceph/ceph/pull/44513>`_, Patrick Donnelly)
* mds: fix seg fault in expire_recursive (`pr#45099 <https://github.com/ceph/ceph/pull/45099>`_, 胡玮文)
* mds: ignore unknown client op when tracking op latency (`pr#44975 <https://github.com/ceph/ceph/pull/44975>`_, Venky Shankar)
* mds: kill session when mds do ms_handle_remote_reset (`issue#53911 <http://tracker.ceph.com/issues/53911>`_, `pr#45100 <https://github.com/ceph/ceph/pull/45100>`_, YunfeiGuan)
* mds: mds_oft_prefetch_dirfrags default to false (`pr#45016 <https://github.com/ceph/ceph/pull/45016>`_, Dan van der Ster)
* mds: opening connection to up:replay/up:creating daemon causes message drop (`pr#44296 <https://github.com/ceph/ceph/pull/44296>`_, Patrick Donnelly)
* mds: PurgeQueue.cc fix for 32bit compilation (`pr#44168 <https://github.com/ceph/ceph/pull/44168>`_, Duncan Bellamy)
* mds: recursive scrub does not trigger stray reintegration (`pr#44514 <https://github.com/ceph/ceph/pull/44514>`_, Patrick Donnelly)
* mds: remove the duplicated or incorrect respond (`pr#44623 <https://github.com/ceph/ceph/pull/44623>`_, Xiubo Li)
* mds: reset heartbeat in each MDSContext complete() (`pr#44551 <https://github.com/ceph/ceph/pull/44551>`_, Xiubo Li)
* mgr/autoscaler: Introduce noautoscale flag (`pr#44540 <https://github.com/ceph/ceph/pull/44540>`_, Kamoltat)
* mgr/cephadm/iscsi: use `mon_command` in `post_remove` instead of `check_mon_command` (`pr#44830 <https://github.com/ceph/ceph/pull/44830>`_, Melissa Li)
* mgr/cephadm: Add client.admin keyring when upgrading from older version (`pr#44625 <https://github.com/ceph/ceph/pull/44625>`_, Sebastian Wagner)
* mgr/cephadm: add keep-alive requests to ssh connections (`pr#45632 <https://github.com/ceph/ceph/pull/45632>`_, Adam King)
* mgr/cephadm: Add snmp-gateway service support (`pr#44529 <https://github.com/ceph/ceph/pull/44529>`_, Sebastian Wagner, Paul Cuzner)
* mgr/cephadm: allow miscellaneous container args at service level (`pr#44829 <https://github.com/ceph/ceph/pull/44829>`_, Adam King)
* mgr/cephadm: auto-enable mirroring module when deploying service (`pr#44661 <https://github.com/ceph/ceph/pull/44661>`_, John Mulligan)
* mgr/cephadm: avoid repeated calls to get_module_option (`pr#44535 <https://github.com/ceph/ceph/pull/44535>`_, Sage Weil)
* mgr/cephadm: block draining last _admin host (`pr#45229 <https://github.com/ceph/ceph/pull/45229>`_, Adam King)
* mgr/cephadm: block removing last instance of _admin label (`pr#45231 <https://github.com/ceph/ceph/pull/45231>`_, Adam King)
* mgr/cephadm: Delete ceph.target if last cluster (`pr#45228 <https://github.com/ceph/ceph/pull/45228>`_, Redouane Kachach)
* mgr/cephadm: extend extra_container_args to other service types (`pr#45234 <https://github.com/ceph/ceph/pull/45234>`_, Adam King)
* mgr/cephadm: fix 'cephadm osd activate' on existing osd devices (`pr#44627 <https://github.com/ceph/ceph/pull/44627>`_, Sage Weil)
* mgr/cephadm: fix 'mgr/cephadm: spec.virtual_ip  param should be used by the ingress daemon (`pr#44628 <https://github.com/ceph/ceph/pull/44628>`_, Guillaume Abrioux, Francesco Pantano, Sebastian Wagner)
* mgr/cephadm: Fix count for OSDs with OSD specs (`pr#44629 <https://github.com/ceph/ceph/pull/44629>`_, Sebastian Wagner)
* mgr/cephadm: fix minor grammar nit in Dry-Runs message (`pr#44637 <https://github.com/ceph/ceph/pull/44637>`_, James McClune)
* mgr/cephadm: fix tcmu-runner cephadm_stray_daemon (`pr#44630 <https://github.com/ceph/ceph/pull/44630>`_, Melissa Li)
* mgr/cephadm: Fix test_facts (`pr#44530 <https://github.com/ceph/ceph/pull/44530>`_, Sebastian Wagner)
* mgr/cephadm: less log noise when config checks fail (`pr#44526 <https://github.com/ceph/ceph/pull/44526>`_, Sage Weil)
* mgr/cephadm: nfs migration: avoid port conflicts (`pr#44631 <https://github.com/ceph/ceph/pull/44631>`_, Sebastian Wagner)
* mgr/cephadm: Show an error when invalid format (`pr#45226 <https://github.com/ceph/ceph/pull/45226>`_, Redouane Kachach)
* mgr/cephadm: store contianer registry credentials in config-key (`pr#44658 <https://github.com/ceph/ceph/pull/44658>`_, Daniel Pivonka)
* mgr/cephadm: try to get FQDN for configuration files (`pr#45620 <https://github.com/ceph/ceph/pull/45620>`_, Tatjana Dehler)
* mgr/cephadm: update monitoring stack versions (`pr#45940 <https://github.com/ceph/ceph/pull/45940>`_, Aashish Sharma, Ernesto Puerta)
* mgr/cephadm: validating service_id for MDS (`pr#45227 <https://github.com/ceph/ceph/pull/45227>`_, Redouane Kachach)
* mgr/dashboard: "Please expand your cluster first" shouldn't be shown if cluster is already meaningfully running (`pr#45044 <https://github.com/ceph/ceph/pull/45044>`_, Volker Theile)
* mgr/dashboard: add test coverage for API docs (SwaggerUI) (`pr#44533 <https://github.com/ceph/ceph/pull/44533>`_, Alfonso Martínez)
* mgr/dashboard: avoid tooltip if disk_usage=null and fast-diff enabled (`pr#44149 <https://github.com/ceph/ceph/pull/44149>`_, Avan Thakkar)
* mgr/dashboard: cephadm e2e job improvements (`pr#44938 <https://github.com/ceph/ceph/pull/44938>`_, Nizamudeen A, Alfonso Martínez)
* mgr/dashboard: cephadm e2e job: improvements (`pr#44382 <https://github.com/ceph/ceph/pull/44382>`_, Alfonso Martínez)
* mgr/dashboard: change privacy protocol field from required to optional (`pr#45052 <https://github.com/ceph/ceph/pull/45052>`_, Avan Thakkar)
* mgr/dashboard: Cluster Expansion - Review Section: fixes and improvements (`pr#44389 <https://github.com/ceph/ceph/pull/44389>`_, Aashish Sharma)
* mgr/dashboard: Compare values of MTU alert by device (`pr#45813 <https://github.com/ceph/ceph/pull/45813>`_, Aashish Sharma, Patrick Seidensal)
* mgr/dashboard: dashboard does not show degraded objects if they are less than 0.5% under "Dashboard->Capacity->Objects block (`pr#44091 <https://github.com/ceph/ceph/pull/44091>`_, Aashish Sharma)
* mgr/dashboard: dashboard turns telemetry off when configuring report (`pr#45111 <https://github.com/ceph/ceph/pull/45111>`_, Sarthak0702, Aaryan Porwal)
* mgr/dashboard: datatable in Cluster Host page hides wrong column on selection (`pr#45861 <https://github.com/ceph/ceph/pull/45861>`_, Sarthak0702)
* mgr/dashboard: Directories Menu Can't Use on Ceph File System Dashboard (`pr#45028 <https://github.com/ceph/ceph/pull/45028>`_, Sarthak0702)
* mgr/dashboard: extend daemon actions to host details (`pr#45721 <https://github.com/ceph/ceph/pull/45721>`_, Nizamudeen A)
* mgr/dashboard: fix api test issue with pip (`pr#45880 <https://github.com/ceph/ceph/pull/45880>`_, Ernesto Puerta)
* mgr/dashboard: fix frontend deps' vulnerabilities (`pr#44297 <https://github.com/ceph/ceph/pull/44297>`_, Alfonso Martínez)
* mgr/dashboard: fix Grafana OSD/host panels (`pr#44775 <https://github.com/ceph/ceph/pull/44775>`_, Patrick Seidensal)
* mgr/dashboard: fix orchestrator/02-hosts-inventory.e2e failure (`pr#44467 <https://github.com/ceph/ceph/pull/44467>`_, Nizamudeen A)
* mgr/dashboard: fix timeout error in dashboard cephadm e2e job (`pr#44468 <https://github.com/ceph/ceph/pull/44468>`_, Nizamudeen A)
* mgr/dashboard: fix white screen on Safari (`pr#45301 <https://github.com/ceph/ceph/pull/45301>`_, 胡玮文)
* mgr/dashboard: fix: get SMART data from single-daemon device (`pr#44597 <https://github.com/ceph/ceph/pull/44597>`_, Alfonso Martínez)
* mgr/dashboard: highlight the search text in cluster logs (`pr#45678 <https://github.com/ceph/ceph/pull/45678>`_, Sarthak0702)
* mgr/dashboard: Implement drain host functionality in dashboard (`pr#44376 <https://github.com/ceph/ceph/pull/44376>`_, Nizamudeen A)
* mgr/dashboard: Improve notifications for osd nearfull, full (`pr#44876 <https://github.com/ceph/ceph/pull/44876>`_, Aashish Sharma)
* mgr/dashboard: Imrove error message of '/api/grafana/validation' API endpoint (`pr#45956 <https://github.com/ceph/ceph/pull/45956>`_, Volker Theile)
* mgr/dashboard: introduce HAProxy metrics for RGW (`pr#44273 <https://github.com/ceph/ceph/pull/44273>`_, Avan Thakkar)
* mgr/dashboard: introduce separate front-end component for API docs (`pr#44400 <https://github.com/ceph/ceph/pull/44400>`_, Aashish Sharma)
* mgr/dashboard: Language dropdown box is partly hidden on login page (`pr#45618 <https://github.com/ceph/ceph/pull/45618>`_, Volker Theile)
* mgr/dashboard: monitoring:Implement BlueStore onode hit/miss counters into the dashboard (`pr#44650 <https://github.com/ceph/ceph/pull/44650>`_, Aashish Sharma)
* mgr/dashboard: NFS non-existent files cleanup (`pr#44046 <https://github.com/ceph/ceph/pull/44046>`_, Alfonso Martínez)
* mgr/dashboard: NFS pages shows 'Page not found' (`pr#45723 <https://github.com/ceph/ceph/pull/45723>`_, Volker Theile)
* mgr/dashboard: Notification banners at the top of the UI have fixed height (`pr#44756 <https://github.com/ceph/ceph/pull/44756>`_, Nizamudeen A, Waad AlKhoury)
* mgr/dashboard: perform daemon actions (`pr#45203 <https://github.com/ceph/ceph/pull/45203>`_, Pere Diaz Bou)
* mgr/dashboard: Pull latest translations from Transifex (`pr#45418 <https://github.com/ceph/ceph/pull/45418>`_, Volker Theile)
* mgr/dashboard: Refactoring dashboard cephadm checks (`pr#44652 <https://github.com/ceph/ceph/pull/44652>`_, Nizamudeen A)
* mgr/dashboard: RGW users and buckets tables are empty if the selected gateway is down (`pr#45868 <https://github.com/ceph/ceph/pull/45868>`_, Volker Theile)
* mgr/dashboard: run-backend-api-tests.sh: Older setuptools (`pr#44377 <https://github.com/ceph/ceph/pull/44377>`_, David Galloway)
* mgr/dashboard: set appropriate baseline branch for applitools (`pr#44935 <https://github.com/ceph/ceph/pull/44935>`_, Nizamudeen A)
* mgr/dashboard: support snmp-gateway service creation from UI (`pr#44977 <https://github.com/ceph/ceph/pull/44977>`_, Avan Thakkar)
* mgr/dashboard: Table columns hiding fix (`issue#51119 <http://tracker.ceph.com/issues/51119>`_, `pr#45725 <https://github.com/ceph/ceph/pull/45725>`_, Daniel Persson)
* mgr/dashboard: Update Angular version to 12 (`pr#44534 <https://github.com/ceph/ceph/pull/44534>`_, Ernesto Puerta, Nizamudeen A)
* mgr/dashboard: upgrade Cypress to the latest stable version (`pr#44086 <https://github.com/ceph/ceph/pull/44086>`_, Sage Weil, Alfonso Martínez)
* mgr/dashboard: use -f for npm ci to skip fsevents error (`pr#44105 <https://github.com/ceph/ceph/pull/44105>`_, Duncan Bellamy)
* mgr/devicehealth: fix missing timezone from time delta calculation (`pr#44325 <https://github.com/ceph/ceph/pull/44325>`_, Yaarit Hatuka)
* mgr/devicehealth: skip null pages when extracting wear level (`pr#45151 <https://github.com/ceph/ceph/pull/45151>`_, Yaarit Hatuka)
* mgr/nfs: allow dynamic update of cephfs nfs export (`pr#45543 <https://github.com/ceph/ceph/pull/45543>`_, Ramana Raja)
* mgr/nfs: support managing exports without orchestration enabled (`pr#45508 <https://github.com/ceph/ceph/pull/45508>`_, John Mulligan)
* mgr/orchestrator: add filtering and count option for orch host ls (`pr#44531 <https://github.com/ceph/ceph/pull/44531>`_, Adam King)
* mgr/prometheus: Added `avail_raw` field for Pools DF Prometheus mgr module (`pr#45236 <https://github.com/ceph/ceph/pull/45236>`_, Konstantin Shalygin)
* mgr/prometheus: define module options for standby (`pr#44205 <https://github.com/ceph/ceph/pull/44205>`_, Sage Weil)
* mgr/prometheus: expose ceph healthchecks as metrics (`pr#44480 <https://github.com/ceph/ceph/pull/44480>`_, Paul Cuzner, Sebastian Wagner)
* mgr/prometheus: Fix metric types from gauge to counter (`pr#43187 <https://github.com/ceph/ceph/pull/43187>`_, Patrick Seidensal)
* mgr/prometheus: Fix the per method stats exported (`pr#44146 <https://github.com/ceph/ceph/pull/44146>`_, Paul Cuzner)
* mgr/prometheus: Make prometheus standby behaviour configurable (`pr#43897 <https://github.com/ceph/ceph/pull/43897>`_, Roland Sommer)
* mgr/rbd_support: cast pool_id from int to str when collecting LevelSpec (`pr#45532 <https://github.com/ceph/ceph/pull/45532>`_, Ilya Dryomov)
* mgr/rbd_support: fix schedule remove (`pr#45005 <https://github.com/ceph/ceph/pull/45005>`_, Sunny Kumar)
* mgr/snap_schedule: backports (`pr#45906 <https://github.com/ceph/ceph/pull/45906>`_, Venky Shankar, Milind Changire)
* mgr/stats: exception handling for ceph fs perf stats command (`pr#44516 <https://github.com/ceph/ceph/pull/44516>`_, Nikhilkumar Shelke)
* mgr/telemetry: fix waiting for mgr to warm up (`pr#45773 <https://github.com/ceph/ceph/pull/45773>`_, Yaarit Hatuka)
* mgr/volumes: A few mgr volumes pacific backports (`pr#45205 <https://github.com/ceph/ceph/pull/45205>`_, Kotresh HR)
* mgr/volumes: Subvolume removal and clone failure fixes (`pr#42932 <https://github.com/ceph/ceph/pull/42932>`_, Kotresh HR)
* mgr/volumes: the 'mode' should honor idempotent subvolume creation (`pr#45474 <https://github.com/ceph/ceph/pull/45474>`_, Nikhilkumar Shelke)
* mgr: Fix ceph_daemon label in ceph_rgw\_\* metrics (`pr#44885 <https://github.com/ceph/ceph/pull/44885>`_, Benoît Knecht)
* mgr: fix locking for MetadataUpdate::finish (`pr#44212 <https://github.com/ceph/ceph/pull/44212>`_, Sage Weil)
* mgr: TTL Cache in mgr module (`pr#44750 <https://github.com/ceph/ceph/pull/44750>`_, Waad AlKhoury, Pere Diaz Bou)
* mgr: various fixes for mgr scalability (`pr#44869 <https://github.com/ceph/ceph/pull/44869>`_, Neha Ojha, Sage Weil)
* mon/MDSMonitor: sanity assert when inline data turned on in MDSMap from v16.2.4 -> v16.2.[567] (`pr#44910 <https://github.com/ceph/ceph/pull/44910>`_, Patrick Donnelly)
* mon/MgrStatMonitor: do not spam subscribers (mgr) with service_map (`pr#44721 <https://github.com/ceph/ceph/pull/44721>`_, Sage Weil)
* mon/MonCommands.h: fix target_size_ratio range (`pr#45397 <https://github.com/ceph/ceph/pull/45397>`_, Kamoltat)
* mon/OSDMonitor: avoid null dereference if stats are not available (`pr#44698 <https://github.com/ceph/ceph/pull/44698>`_, Josh Durgin)
* mon: Abort device health when device not found (`pr#44959 <https://github.com/ceph/ceph/pull/44959>`_, Benoît Knecht)
* mon: do not quickly mark mds laggy when MON_DOWN (`pr#43698 <https://github.com/ceph/ceph/pull/43698>`_, Sage Weil, Patrick Donnelly)
* mon: Omit MANY_OBJECTS_PER_PG warning when autoscaler is on (`pr#45152 <https://github.com/ceph/ceph/pull/45152>`_, Christopher Hoffman)
* mon: osd pool create <pool-name> with --bulk flag (`pr#44847 <https://github.com/ceph/ceph/pull/44847>`_, Kamoltat)
* mon: prevent new sessions during shutdown (`pr#44543 <https://github.com/ceph/ceph/pull/44543>`_, Sage Weil)
* monitoring/grafana: Grafana query tester (`pr#44316 <https://github.com/ceph/ceph/pull/44316>`_, Ernesto Puerta, Pere Diaz Bou)
* monitoring: mention PyYAML only once in requirements (`pr#44944 <https://github.com/ceph/ceph/pull/44944>`_, Rishabh Dave)
* os/bluestore/AvlAllocator: introduce bluestore_avl_alloc_ff_max\_\* options (`pr#43745 <https://github.com/ceph/ceph/pull/43745>`_, Kefu Chai, Mauricio Faria de Oliveira, Adam Kupczyk)
* os/bluestore: avoid premature onode release (`pr#44723 <https://github.com/ceph/ceph/pull/44723>`_, Igor Fedotov)
* os/bluestore: make shared blob fsck much less RAM-greedy (`pr#44613 <https://github.com/ceph/ceph/pull/44613>`_, Igor Fedotov)
* os/bluestore: use proper prefix when removing undecodable Share Blob (`pr#43882 <https://github.com/ceph/ceph/pull/43882>`_, Igor Fedotov)
* osd/OSD: Log aggregated slow ops detail to cluster logs (`pr#44771 <https://github.com/ceph/ceph/pull/44771>`_, Prashant D)
* osd/OSDMap.cc: clean up pg_temp for nonexistent pgs (`pr#44096 <https://github.com/ceph/ceph/pull/44096>`_, Cory Snyder)
* osd/OSDMap: Add health warning if 'require-osd-release' != current release (`pr#44259 <https://github.com/ceph/ceph/pull/44259>`_, Sridhar Seshasayee, Patrick Donnelly, Neha Ojha)
* osd/OSDMapMapping: fix spurious threadpool timeout errors (`pr#44545 <https://github.com/ceph/ceph/pull/44545>`_, Sage Weil)
* osd/PeeringState: separate history's pruub from pg's (`pr#44584 <https://github.com/ceph/ceph/pull/44584>`_, Sage Weil)
* osd/PrimaryLogPG.cc: CEPH_OSD_OP_OMAPRMKEYRANGE should mark omap dirty (`pr#45591 <https://github.com/ceph/ceph/pull/45591>`_, Neha Ojha)
* osd/scrub: destruct the scrubber shortly before the PG is destructed (`pr#45731 <https://github.com/ceph/ceph/pull/45731>`_, Ronen Friedman)
* osd/scrub: only telling the scrubber of awaited-for 'updates' events (`pr#45365 <https://github.com/ceph/ceph/pull/45365>`_, Ronen Friedman)
* osd/scrub: remove reliance of Scrubber objects' logging on the PG (`pr#45729 <https://github.com/ceph/ceph/pull/45729>`_, Ronen Friedman)
* osd/scrub: restart snap trimming only after scrubbing is done (`pr#45785 <https://github.com/ceph/ceph/pull/45785>`_, Ronen Friedman)
* osd/scrub: stop sending bogus digest-update events (`issue#54423 <http://tracker.ceph.com/issues/54423>`_, `pr#45194 <https://github.com/ceph/ceph/pull/45194>`_, Ronen Friedman)
* osd/scrub: tag replica scrub messages to identify stale events (`pr#45374 <https://github.com/ceph/ceph/pull/45374>`_, Ronen Friedman)
* osd: add pg_num_max value & pg_num_max reordering (`pr#45173 <https://github.com/ceph/ceph/pull/45173>`_, Kamoltat, Sage Weil)
* osd: fix 'ceph osd stop <osd.nnn>' doesn't take effect (`pr#43955 <https://github.com/ceph/ceph/pull/43955>`_, tan changzhi)
* osd: fix the truncation of an int by int division (`pr#45376 <https://github.com/ceph/ceph/pull/45376>`_, Ronen Friedman)
* osd: PeeringState: fix selection order in calc_replicated_acting_stretch (`pr#44664 <https://github.com/ceph/ceph/pull/44664>`_, Greg Farnum)
* osd: recover unreadable snapshot before reading ref. count info (`pr#44181 <https://github.com/ceph/ceph/pull/44181>`_, Myoungwon Oh)
* osd: require osd_pg_max_concurrent_snap_trims > 0 (`pr#45323 <https://github.com/ceph/ceph/pull/45323>`_, Dan van der Ster)
* osd: set r only if succeed in FillInVerifyExtent (`pr#44173 <https://github.com/ceph/ceph/pull/44173>`_, yanqiang-ux)
* osdc: add set_error in BufferHead, when split set_error to right (`pr#44725 <https://github.com/ceph/ceph/pull/44725>`_, jiawd)
* pacfic: doc/rados/operations/placement-groups: fix --bulk docs (`pr#45328 <https://github.com/ceph/ceph/pull/45328>`_, Kamoltat)
* Pacific fast shutdown backports (`pr#45654 <https://github.com/ceph/ceph/pull/45654>`_, Sridhar Seshasayee, Nitzan Mordechai, Satoru Takeuchi)
* pybind/mgr/balancer: define Plan.{dump,show}() (`pr#43964 <https://github.com/ceph/ceph/pull/43964>`_, Kefu Chai)
* pybind/mgr/progress: enforced try and except on accessing event dictionary (`pr#44672 <https://github.com/ceph/ceph/pull/44672>`_, Kamoltat)
* python-common: add int value validation for count and count_per_host (`pr#44527 <https://github.com/ceph/ceph/pull/44527>`_, John Mulligan)
* python-common: improve OSD spec error messages (`pr#44626 <https://github.com/ceph/ceph/pull/44626>`_, Sebastian Wagner)
* qa/distros/podman: remove centos_8.2 and centos_8.3 (`pr#44903 <https://github.com/ceph/ceph/pull/44903>`_, Neha Ojha)
* qa/rgw: add failing tempest test to blocklist (`pr#45436 <https://github.com/ceph/ceph/pull/45436>`_, Casey Bodley)
* qa/rgw: barbican and pykmip tasks upgrade pip before installing pytz (`pr#45444 <https://github.com/ceph/ceph/pull/45444>`_, Casey Bodley)
* qa/rgw: bump tempest version to resolve dependency issue (`pr#43966 <https://github.com/ceph/ceph/pull/43966>`_, Casey Bodley)
* qa/rgw: Fix vault token file access (`issue#51539 <http://tracker.ceph.com/issues/51539>`_, `pr#43951 <https://github.com/ceph/ceph/pull/43951>`_, Marcus Watts)
* qa/rgw: update apache-maven mirror for rgw/hadoop-s3a (`pr#45445 <https://github.com/ceph/ceph/pull/45445>`_, Casey Bodley)
* qa/rgw: use symlinks for rgw/sts suite, target supported-random-distro$ (`pr#45245 <https://github.com/ceph/ceph/pull/45245>`_, Casey Bodley)
* qa/run-tox-mgr-dashboard: Do not write to /tmp/test_sanitize_password… (`pr#44727 <https://github.com/ceph/ceph/pull/44727>`_, Kevin Zhao)
* qa/run_xfstests_qemu.sh: stop reporting success without actually running any tests (`pr#44596 <https://github.com/ceph/ceph/pull/44596>`_, Ilya Dryomov)
* qa/suites/fs: add prefetch_dirfrags false to thrasher suite (`pr#44504 <https://github.com/ceph/ceph/pull/44504>`_, Arthur Outhenin-Chalandre)
* qa/suites/orch/cephadm: Also run the rbd/iscsi suite (`pr#44635 <https://github.com/ceph/ceph/pull/44635>`_, Sebastian Wagner)
* qa/tasks/qemu: make sure block-rbd.so is installed (`pr#45072 <https://github.com/ceph/ceph/pull/45072>`_, Ilya Dryomov)
* qa/tasks: improve backfill_toofull test (`pr#44387 <https://github.com/ceph/ceph/pull/44387>`_, Mykola Golub)
* qa/tests: added upgrade-clients/client-upgrade-pacific-quincy test (`pr#45326 <https://github.com/ceph/ceph/pull/45326>`_, Yuri Weinstein)
* qa/tests: replaced 16.2.6 with 16.2.7 version (`pr#44369 <https://github.com/ceph/ceph/pull/44369>`_, Yuri Weinstein)
* qa: adjust for MDSs to get deployed before verifying their availability (`issue#53857 <http://tracker.ceph.com/issues/53857>`_, `pr#44639 <https://github.com/ceph/ceph/pull/44639>`_, Venky Shankar)
* qa: Default to CentOS 8 Stream (`pr#44889 <https://github.com/ceph/ceph/pull/44889>`_, David Galloway)
* qa: do not use any time related suffix for \*_op_timeouts (`pr#44621 <https://github.com/ceph/ceph/pull/44621>`_, Xiubo Li)
* qa: fsync dir for asynchronous creat on stray tests (`pr#45565 <https://github.com/ceph/ceph/pull/45565>`_, Patrick Donnelly, Ramana Raja)
* qa: ignore expected metadata cluster log error (`pr#45564 <https://github.com/ceph/ceph/pull/45564>`_, Patrick Donnelly)
* qa: increase the timeout value to wait a litte longer (`pr#43979 <https://github.com/ceph/ceph/pull/43979>`_, Xiubo Li)
* qa: move certificates for kmip task into /etc/ceph (`pr#45413 <https://github.com/ceph/ceph/pull/45413>`_, Ali Maredia)
* qa: remove centos8 from supported distros (`pr#44865 <https://github.com/ceph/ceph/pull/44865>`_, Casey Bodley, Sage Weil)
* qa: skip sanity check during upgrade (`pr#44840 <https://github.com/ceph/ceph/pull/44840>`_, Milind Changire)
* qa: split distro for rados/cephadm/smoke tests (`pr#44681 <https://github.com/ceph/ceph/pull/44681>`_, Guillaume Abrioux)
* qa: wait for purge queue operations to finish (`issue#52487 <http://tracker.ceph.com/issues/52487>`_, `pr#44642 <https://github.com/ceph/ceph/pull/44642>`_, Venky Shankar)
* radosgw-admin: 'sync status' is not behind if there are no mdlog entries (`pr#45442 <https://github.com/ceph/ceph/pull/45442>`_, Casey Bodley)
* rbd persistent cache UX improvements (status report, metrics, flush command) (`pr#45895 <https://github.com/ceph/ceph/pull/45895>`_, Ilya Dryomov, Yin Congmin)
* rbd-mirror: fix races in snapshot-based mirroring deletion propagation (`pr#44754 <https://github.com/ceph/ceph/pull/44754>`_, Ilya Dryomov)
* rbd-mirror: make mirror properly detect pool replayer needs restart (`pr#45170 <https://github.com/ceph/ceph/pull/45170>`_, Mykola Golub)
* rbd-mirror: make RemoveImmediateUpdate test synchronous (`pr#44094 <https://github.com/ceph/ceph/pull/44094>`_, Arthur Outhenin-Chalandre)
* rbd-mirror: synchronize with in-flight stop in ImageReplayer::stop() (`pr#45184 <https://github.com/ceph/ceph/pull/45184>`_, Ilya Dryomov)
* rbd: add missing switch arguments for recognition by get_command_spec() (`pr#44742 <https://github.com/ceph/ceph/pull/44742>`_, Ilya Dryomov)
* rbd: mark optional positional arguments as such in help output (`pr#45008 <https://github.com/ceph/ceph/pull/45008>`_, Ilya Dryomov)
* rbd: recognize rxbounce map option (`pr#45002 <https://github.com/ceph/ceph/pull/45002>`_, Ilya Dryomov)
* Revert "mds: kill session when mds do ms_handle_remote_reset" (`pr#45557 <https://github.com/ceph/ceph/pull/45557>`_, Venky Shankar)
* revert bootstrap network handling changes (`pr#46085 <https://github.com/ceph/ceph/pull/46085>`_, Adam King)
* revival and backport of fix for RocksDB optimized iterators (`pr#46096 <https://github.com/ceph/ceph/pull/46096>`_, Adam Kupczyk, Cory Snyder)
* RGW - Zipper - Make default args match in get_obj_state (`pr#45438 <https://github.com/ceph/ceph/pull/45438>`_, Daniel Gryniewicz)
* RGW - Zipper - Make sure PostObj has bucket set (`pr#45060 <https://github.com/ceph/ceph/pull/45060>`_, Daniel Gryniewicz)
* rgw/admin: fix radosgw-admin datalog list max-entries issue (`pr#45500 <https://github.com/ceph/ceph/pull/45500>`_, Yuval Lifshitz)
* rgw/amqp: add default case to silence compiler warning (`pr#45478 <https://github.com/ceph/ceph/pull/45478>`_, Casey Bodley)
* rgw/amqp: remove the explicit "disconnect()" interface (`pr#45427 <https://github.com/ceph/ceph/pull/45427>`_, Yuval Lifshitz)
* rgw/beast: optimizations for request timeout (`pr#43946 <https://github.com/ceph/ceph/pull/43946>`_, Mark Kogan, Casey Bodley)
* rgw/notification: send correct size in COPY events (`pr#45426 <https://github.com/ceph/ceph/pull/45426>`_, Yuval Lifshitz)
* rgw/sts: adding role name and role session to ops log (`pr#43956 <https://github.com/ceph/ceph/pull/43956>`_, Pritha Srivastava)
* rgw: add object null point judging when listing pubsub  topics (`pr#45476 <https://github.com/ceph/ceph/pull/45476>`_, zhipeng li)
* rgw: add OPT_BUCKET_SYNC_RUN to gc_ops_list, so that (`pr#45421 <https://github.com/ceph/ceph/pull/45421>`_, Pritha Srivastava)
* rgw: add the condition of lock mode conversion to PutObjRentention (`pr#45440 <https://github.com/ceph/ceph/pull/45440>`_, wangzhong)
* rgw: bucket chown bad memory usage (`pr#45491 <https://github.com/ceph/ceph/pull/45491>`_, Mohammad Fatemipour)
* rgw: change order of xml elements in ListRoles response (`pr#45448 <https://github.com/ceph/ceph/pull/45448>`_, Casey Bodley)
* rgw: clean-up logging of function entering to make thorough and consistent (`pr#45450 <https://github.com/ceph/ceph/pull/45450>`_, J. Eric Ivancich)
* rgw: cls_bucket_list_unordered() might return one redundent entry every time is_truncated is true (`pr#45457 <https://github.com/ceph/ceph/pull/45457>`_, Peng Zhang)
* rgw: default ms_mon_client_mode = secure (`pr#45439 <https://github.com/ceph/ceph/pull/45439>`_, Sage Weil)
* rgw: document rgw_lc_debug_interval configuration option (`pr#45453 <https://github.com/ceph/ceph/pull/45453>`_, J. Eric Ivancich)
* rgw: document S3 bucket replication support (`pr#45484 <https://github.com/ceph/ceph/pull/45484>`_, Matt Benjamin)
* rgw: Dump Object Lock Retain Date as ISO 8601 (`pr#44697 <https://github.com/ceph/ceph/pull/44697>`_, Danny Abukalam)
* rgw: fix `bi put` not using right bucket index shard (`pr#44166 <https://github.com/ceph/ceph/pull/44166>`_, J. Eric Ivancich)
* rgw: fix lock scope in ObjectCache::get() (`pr#44747 <https://github.com/ceph/ceph/pull/44747>`_, Casey Bodley)
* rgw: fix md5 not match for RGWBulkUploadOp upload when enable rgw com… (`pr#45432 <https://github.com/ceph/ceph/pull/45432>`_, yuliyang_yewu)
* rgw: fix rgw.none statistics (`pr#45463 <https://github.com/ceph/ceph/pull/45463>`_, J. Eric Ivancich)
* rgw: fix segfault in UserAsyncRefreshHandler::init_fetch (`pr#45411 <https://github.com/ceph/ceph/pull/45411>`_, Cory Snyder)
* rgw: forward request in multisite for RGWDeleteBucketPolicy and RGWDeleteBucketPublicAccessBlock (`pr#45434 <https://github.com/ceph/ceph/pull/45434>`_, yuliyang_yewu)
* rgw: have "bucket check --fix" fix pool ids correctly (`pr#45455 <https://github.com/ceph/ceph/pull/45455>`_, J. Eric Ivancich)
* rgw: in bucket reshard list, clarify new num shards is tentative (`pr#45509 <https://github.com/ceph/ceph/pull/45509>`_, J. Eric Ivancich)
* rgw: init bucket index only if putting bucket instance info succeeds (`pr#45480 <https://github.com/ceph/ceph/pull/45480>`_, Huber-ming)
* rgw: RadosBucket::get_bucket_info() updates RGWBucketEnt (`pr#45483 <https://github.com/ceph/ceph/pull/45483>`_, Casey Bodley)
* rgw: remove bucket API returns NoSuchKey than NoSuchBucket (`pr#45489 <https://github.com/ceph/ceph/pull/45489>`_, Satoru Takeuchi)
* rgw: resolve empty ordered bucket listing results w/ CLS filtering \*and\* bucket index list produces incorrect result when non-ascii entries (`pr#45087 <https://github.com/ceph/ceph/pull/45087>`_, J. Eric Ivancich)
* rgw: RGWPostObj::execute() may lost data (`pr#45502 <https://github.com/ceph/ceph/pull/45502>`_, Lei Zhang)
* rgw: under fips, set flag to allow md5 in select rgw ops (`pr#44778 <https://github.com/ceph/ceph/pull/44778>`_, Mark Kogan)
* rgw: url_decode before parsing copysource in copyobject (`issue#43259 <http://tracker.ceph.com/issues/43259>`_, `pr#45430 <https://github.com/ceph/ceph/pull/45430>`_, Paul Reece)
* rgw: user stats showing 0 value for "size_utilized" and "size_kb_utilized" fields (`pr#44171 <https://github.com/ceph/ceph/pull/44171>`_, J. Eric Ivancich)
* rgw: write meta of a MP part to a correct pool (`issue#49128 <http://tracker.ceph.com/issues/49128>`_, `pr#45428 <https://github.com/ceph/ceph/pull/45428>`_, Jeegn Chen)
* rgw:When KMS encryption is used and the key does not exist, we should… (`pr#45461 <https://github.com/ceph/ceph/pull/45461>`_, wangyingbin)
* rgwlc:  remove lc entry on bucket delete (`pr#44729 <https://github.com/ceph/ceph/pull/44729>`_, Matt Benjamin)
* rgwlc:  warn on missing RGW_ATTR_LC (`pr#45497 <https://github.com/ceph/ceph/pull/45497>`_, Matt Benjamin)
* src/ceph-crash.in: various enhancements and fixes (`pr#45381 <https://github.com/ceph/ceph/pull/45381>`_, Sébastien Han)
* src/rgw: Fix for malformed url (`pr#45459 <https://github.com/ceph/ceph/pull/45459>`_, Kalpesh Pandya)
* test/librbd/test_notify.py: effect post object map rebuild assert (`pr#45311 <https://github.com/ceph/ceph/pull/45311>`_, Ilya Dryomov)
* test/librbd: add test to verify diff_iterate size (`pr#45555 <https://github.com/ceph/ceph/pull/45555>`_, Christopher Hoffman)
* test/librbd: harden RemoveFullTry tests (`pr#43649 <https://github.com/ceph/ceph/pull/43649>`_, Ilya Dryomov)
* test/rgw: disable cls_rgw_gc test cases with defer_gc() (`pr#45477 <https://github.com/ceph/ceph/pull/45477>`_, Casey Bodley)
* test: fix wrong alarm (HitSetWrite) (`pr#45319 <https://github.com/ceph/ceph/pull/45319>`_, Myoungwon Oh)
* test: increase retry duration when calculating manifest ref. count (`pr#44202 <https://github.com/ceph/ceph/pull/44202>`_, Myoungwon Oh)
* tools/rbd: expand where option rbd_default_map_options can be set (`pr#45181 <https://github.com/ceph/ceph/pull/45181>`_, Christopher Hoffman, Ilya Dryomov)
* Wip doc pr 46109 backport to pacific (`pr#46117 <https://github.com/ceph/ceph/pull/46117>`_, Ville Ojamo)


v16.2.7 Pacific   
===============

This is the seventh backport release in the Pacific series.

Notable Changes
---------------

* Critical bug in OMAP format upgrade is fixed. This could cause data corruption
  (improperly formatted OMAP keys) after pre-Pacific cluster upgrade if
  bluestore-quick-fix-on-mount parameter is set to true or ceph-bluestore-tool's
  quick-fix/repair commands are invoked.
  Relevant tracker: https://tracker.ceph.com/issues/53062
  ``bluestore-quick-fix-on-mount`` continues to be set to false, by default.
  
* CephFS:  If you are not using cephadm, you must disable FSMap sanity checks *before starting the upgrade*::
  
      ceph config set mon mon_mds_skip_sanity true

  After the upgrade has finished and the cluster is stable, please remove that setting::

      ceph config rm mon mon_mds_skip_sanity

  Clusters managed by and upgraded using cephadm take care of this step automatically.

* MGR: The pg_autoscaler will use the 'scale-up' profile as the default profile.
  16.2.6 changed the default profile to 'scale-down' but we ran into issues
  with the device_health_metrics pool consuming too many PGs, which is not ideal
  for performance. So we will continue to use the 'scale-up' profile by default,
  until we implement a limit on the number of PGs default pools should consume,
  in combination with the 'scale-down' profile.

* Cephadm & Ceph Dashboard: NFS management has been completely reworked to
  ensure that NFS exports are managed consistently across the different Ceph
  components. Prior to this, there were 3 incompatible implementations for
  configuring the NFS exports: Ceph-Ansible/OpenStack Manila, Ceph Dashboard and
  'mgr/nfs' module. With this release the 'mgr/nfs' way becomes the official
  interface, and the remaining components (Cephadm and Ceph Dashboard) adhere to
  it. While this might require manually migrating from the deprecated
  implementations, it will simplify the user experience for those heavily
  relying on NFS exports.

* Dashboard: "Cluster Expansion Wizard". After the 'cephadm bootstrap' step,
  users that log into the Ceph Dashboard will be presented with a welcome
  screen. If they choose to follow the installation wizard, they will be guided 
  through a set of steps to help them configure their Ceph cluster: expanding
  the cluster by adding more hosts, detecting and defining their storage
  devices, and finally deploying and configuring the different Ceph services.

* OSD: When using mclock_scheduler for QoS, there is no longer a need to run any
  manual benchmark. The OSD now automatically sets an appropriate value for
  `osd_mclock_max_capacity_iops` by running a simple benchmark during
  initialization.

* MGR: The global recovery event in the progress module has been optimized and
  a `sleep_interval` of 5 seconds has been added between stats collection,
  to reduce the impact of the progress module on the MGR, especially in large
  clusters.
  
Changelog
---------

* rpm, debian: move smartmontools and nvme-cli to ceph-base (`pr#44164 <https://github.com/ceph/ceph/pull/44164>`_, Yaarit Hatuka)
* qa: miscellaneous perf suite fixes (`pr#44154 <https://github.com/ceph/ceph/pull/44154>`_, Neha Ojha)
* qa/suites/orch/cephadm: mgr-nfs-upgrade: add missing 0-distro dir (`pr#44201 <https://github.com/ceph/ceph/pull/44201>`_, Sebastian Wagner)
* \*: s/virtualenv/python -m venv/ (`pr#43002 <https://github.com/ceph/ceph/pull/43002>`_, Kefu Chai, Ken Dreyer)
* admin/doc-requirements.txt: pin Sphinx at 3.5.4 (`pr#43748 <https://github.com/ceph/ceph/pull/43748>`_, Kefu Chai)
* backport mgr/nfs bits (`pr#43811 <https://github.com/ceph/ceph/pull/43811>`_, Sage Weil, Michael Fritch)
* ceph-volume: `get_first_lv()` refactor (`pr#43960 <https://github.com/ceph/ceph/pull/43960>`_, Guillaume Abrioux)
* ceph-volume: fix a typo causing AttributeError (`pr#43949 <https://github.com/ceph/ceph/pull/43949>`_, Taha Jahangir)
* ceph-volume: fix bug with miscalculation of required db/wal slot size for VGs with multiple PVs (`pr#43948 <https://github.com/ceph/ceph/pull/43948>`_, Guillaume Abrioux, Cory Snyder)
* ceph-volume: fix lvm activate --all --no-systemd (`pr#43267 <https://github.com/ceph/ceph/pull/43267>`_, Dimitri Savineau)
* ceph-volume: util/prepare fix osd_id_available() (`pr#43708 <https://github.com/ceph/ceph/pull/43708>`_, Guillaume Abrioux)
* ceph.spec: selinux scripts respect CEPH_AUTO_RESTART_ON_UPGRADE (`pr#43235 <https://github.com/ceph/ceph/pull/43235>`_, Dan van der Ster)
* cephadm: November batch (`pr#43906 <https://github.com/ceph/ceph/pull/43906>`_, Sebastian Wagner, Sage Weil, Daniel Pivonka, Andrew Sharapov, Paul Cuzner, Adam King, Melissa Li)
* cephadm: October batch (`pr#43728 <https://github.com/ceph/ceph/pull/43728>`_, Patrick Donnelly, Sage Weil, Cory Snyder, Sebastian Wagner, Paul Cuzner, Joao Eduardo Luis, Zac Dover, Dmitry Kvashnin, Daniel Pivonka, Adam King, jianglong01, Guillaume Abrioux, Melissa Li, Roaa Sakr, Kefu Chai, Brad Hubbard, Michael Fritch, Javier Cacheiro)
* cephfs-mirror, test: add thrasher for cephfs mirror daemon, HA test yamls (`issue#50372 <http://tracker.ceph.com/issues/50372>`_, `pr#43924 <https://github.com/ceph/ceph/pull/43924>`_, Venky Shankar)
* cephfs-mirror: shutdown ClusterWatcher on termination (`pr#43198 <https://github.com/ceph/ceph/pull/43198>`_, Willem Jan Withagen, Venky Shankar)
* cmake: link Threads::Threads instead of CMAKE_THREAD_LIBS_INIT (`pr#43167 <https://github.com/ceph/ceph/pull/43167>`_, Ken Dreyer)
* cmake: s/Python_EXECUTABLE/Python3_EXECUTABLE/ (`pr#43264 <https://github.com/ceph/ceph/pull/43264>`_, Michael Fritch)
* crush: cancel upmaps with up set size != pool size (`pr#43415 <https://github.com/ceph/ceph/pull/43415>`_, huangjun)
* doc/radosgw/nfs: add note about NFSv3 deprecation (`pr#43941 <https://github.com/ceph/ceph/pull/43941>`_, Michael Fritch)
* doc: document subvolume (group) pins (`pr#43925 <https://github.com/ceph/ceph/pull/43925>`_, Patrick Donnelly)
* github: add dashboard PRs to Dashboard project (`pr#43610 <https://github.com/ceph/ceph/pull/43610>`_, Ernesto Puerta)
* librbd/cache/pwl: persistant cache backports (`pr#43772 <https://github.com/ceph/ceph/pull/43772>`_, Kefu Chai, Yingxin Cheng, Yin Congmin, Feng Hualong, Jianpeng Ma, Ilya Dryomov, Hualong Feng)
* librbd/cache/pwl: SSD caching backports (`pr#43918 <https://github.com/ceph/ceph/pull/43918>`_, Yin Congmin, Jianpeng Ma)
* librbd/object_map: rbd diff between two snapshots lists entire image content (`pr#43805 <https://github.com/ceph/ceph/pull/43805>`_, Sunny Kumar)
* librbd: fix pool validation lockup (`pr#43113 <https://github.com/ceph/ceph/pull/43113>`_, Ilya Dryomov)
* mds/FSMap: do not assert allow_standby_replay on old FSMaps (`pr#43614 <https://github.com/ceph/ceph/pull/43614>`_, Patrick Donnelly)
* mds: Add new flag to MClientSession (`pr#43251 <https://github.com/ceph/ceph/pull/43251>`_, Kotresh HR)
* mds: do not trim stray dentries during opening the root (`pr#43815 <https://github.com/ceph/ceph/pull/43815>`_, Xiubo Li)
* mds: skip journaling blocklisted clients when in `replay` state (`pr#43841 <https://github.com/ceph/ceph/pull/43841>`_, Venky Shankar)
* mds: switch mds_lock to fair mutex to fix the slow performance issue (`pr#43148 <https://github.com/ceph/ceph/pull/43148>`_, Xiubo Li, Kefu Chai)
* MDSMonitor: assertion during upgrade to v16.2.5+ (`pr#43890 <https://github.com/ceph/ceph/pull/43890>`_, Patrick Donnelly)
* MDSMonitor: handle damaged state from standby-replay (`pr#43200 <https://github.com/ceph/ceph/pull/43200>`_, Patrick Donnelly)
* MDSMonitor: no active MDS after cluster deployment (`pr#43891 <https://github.com/ceph/ceph/pull/43891>`_, Patrick Donnelly)
* mgr/dashboard,prometheus: fix handling of server_addr (`issue#52002 <http://tracker.ceph.com/issues/52002>`_, `pr#43631 <https://github.com/ceph/ceph/pull/43631>`_, Scott Shambarger)
* mgr/dashboard: all pyfakefs must be pinned on same version (`pr#43930 <https://github.com/ceph/ceph/pull/43930>`_, Rishabh Dave)
* mgr/dashboard: BATCH incl.: NFS integration, Cluster Expansion Workflow, and Angular 11 upgrade (`pr#43682 <https://github.com/ceph/ceph/pull/43682>`_, Alfonso Martínez, Avan Thakkar, Aashish Sharma, Nizamudeen A, Pere Diaz Bou, Varsha Rao, Ramana Raja, Sage Weil, Kefu Chai)
* mgr/dashboard: cephfs MDS Workload to use rate for counter type metric (`pr#43190 <https://github.com/ceph/ceph/pull/43190>`_, Jan Horacek)
* mgr/dashboard: clean-up controllers and API backward versioning compatibility (`pr#43543 <https://github.com/ceph/ceph/pull/43543>`_, Ernesto Puerta, Avan Thakkar)
* mgr/dashboard: Daemon Events listing using bootstrap class (`pr#44057 <https://github.com/ceph/ceph/pull/44057>`_, Nizamudeen A)
* mgr/dashboard: deprecated variable usage in Grafana dashboards (`pr#43188 <https://github.com/ceph/ceph/pull/43188>`_, Patrick Seidensal)
* mgr/dashboard: Device health status is not getting listed under hosts section (`pr#44053 <https://github.com/ceph/ceph/pull/44053>`_, Aashish Sharma)
* mgr/dashboard: Edit a service feature (`pr#43939 <https://github.com/ceph/ceph/pull/43939>`_, Nizamudeen A)
* mgr/dashboard: Fix failing config dashboard e2e check (`pr#43238 <https://github.com/ceph/ceph/pull/43238>`_, Nizamudeen A)
* mgr/dashboard: fix flaky inventory e2e test (`pr#44056 <https://github.com/ceph/ceph/pull/44056>`_, Nizamudeen A)
* mgr/dashboard: fix missing alert rule details (`pr#43812 <https://github.com/ceph/ceph/pull/43812>`_, Ernesto Puerta)
* mgr/dashboard: Fix orchestrator/01-hosts.e2e-spec.ts failure (`pr#43541 <https://github.com/ceph/ceph/pull/43541>`_, Nizamudeen A)
* mgr/dashboard: include mfa_ids in rgw user-details section (`pr#43893 <https://github.com/ceph/ceph/pull/43893>`_, Avan Thakkar)
* mgr/dashboard: Incorrect MTU mismatch warning (`pr#43185 <https://github.com/ceph/ceph/pull/43185>`_, Aashish Sharma)
* mgr/dashboard: monitoring: grafonnet refactoring for radosgw dashboards (`pr#43644 <https://github.com/ceph/ceph/pull/43644>`_, Aashish Sharma)
* mgr/dashboard: Move force maintenance test to the workflow test suite (`pr#43347 <https://github.com/ceph/ceph/pull/43347>`_, Nizamudeen A)
* mgr/dashboard: pin a version for autopep8 and pyfakefs (`pr#43646 <https://github.com/ceph/ceph/pull/43646>`_, Nizamudeen A)
* mgr/dashboard: Predefine labels in create host form (`pr#44077 <https://github.com/ceph/ceph/pull/44077>`_, Nizamudeen A)
* mgr/dashboard: provisioned values is misleading in RBD image table (`pr#44051 <https://github.com/ceph/ceph/pull/44051>`_, Avan Thakkar)
* mgr/dashboard: replace "Ceph-cluster" Client connections with active-standby MGRs (`pr#43523 <https://github.com/ceph/ceph/pull/43523>`_, Avan Thakkar)
* mgr/dashboard: rgw daemon list: add realm column (`pr#44047 <https://github.com/ceph/ceph/pull/44047>`_, Alfonso Martínez)
* mgr/dashboard: Spelling mistake in host-form Network address field (`pr#43973 <https://github.com/ceph/ceph/pull/43973>`_, Avan Thakkar)
* mgr/dashboard: Visual regression tests for ceph dashboard (`pr#42678 <https://github.com/ceph/ceph/pull/42678>`_, Aaryan Porwal)
* mgr/dashboard: visual tests: Add more ignore regions for dashboard component (`pr#43240 <https://github.com/ceph/ceph/pull/43240>`_, Aaryan Porwal)
* mgr/influx: use "N/A" for unknown hostname (`pr#43368 <https://github.com/ceph/ceph/pull/43368>`_, Kefu Chai)
* mgr/mirroring: remove unnecessary fs_name arg from daemon status command (`issue#51989 <http://tracker.ceph.com/issues/51989>`_, `pr#43199 <https://github.com/ceph/ceph/pull/43199>`_, Venky Shankar)
* mgr/nfs: nfs-rgw batch backport (`pr#43075 <https://github.com/ceph/ceph/pull/43075>`_, Sebastian Wagner, Sage Weil, Varsha Rao, Ramana Raja)
* mgr/progress: optimize global recovery && introduce 5 seconds interval (`pr#43353 <https://github.com/ceph/ceph/pull/43353>`_, Kamoltat, Neha Ojha)
* mgr/prometheus: offer ability to disable cache (`pr#43931 <https://github.com/ceph/ceph/pull/43931>`_, Patrick Seidensal)
* mgr/volumes: Fix permission during subvol creation with mode (`pr#43223 <https://github.com/ceph/ceph/pull/43223>`_, Kotresh HR)
* mgr: Add check to prevent mgr from crashing (`pr#43445 <https://github.com/ceph/ceph/pull/43445>`_, Aswin Toni)
* mon,auth: fix proposal (and mon db rebuild) of rotating secrets (`pr#43697 <https://github.com/ceph/ceph/pull/43697>`_, Sage Weil)
* mon/MDSMonitor: avoid crash when decoding old FSMap epochs (`pr#43615 <https://github.com/ceph/ceph/pull/43615>`_, Patrick Donnelly)
* mon: Allow specifying new tiebreaker monitors (`pr#43457 <https://github.com/ceph/ceph/pull/43457>`_, Greg Farnum)
* mon: MonMap: display disallowed_leaders whenever they're set (`pr#43972 <https://github.com/ceph/ceph/pull/43972>`_, Greg Farnum)
* mon: MonMap: do not increase mon_info_t's compatv in stretch mode, really (`pr#43971 <https://github.com/ceph/ceph/pull/43971>`_, Greg Farnum)
* monitoring: ethernet bonding filter in Network Load (`pr#43694 <https://github.com/ceph/ceph/pull/43694>`_, Pere Diaz Bou)
* msg/async/ProtocolV2: Set the recv_stamp at the beginning of receiving a message (`pr#43511 <https://github.com/ceph/ceph/pull/43511>`_, dongdong tao)
* msgr/async: fix unsafe access in unregister_conn() (`pr#43548 <https://github.com/ceph/ceph/pull/43548>`_, Sage Weil, Radoslaw Zarzynski)
* os/bluestore: _do_write_small fix head_pad (`pr#43756 <https://github.com/ceph/ceph/pull/43756>`_, dheart)
* os/bluestore: do not select absent device in volume selector (`pr#43970 <https://github.com/ceph/ceph/pull/43970>`_, Igor Fedotov)
* os/bluestore: fix invalid omap name conversion when upgrading to per-pg (`pr#43793 <https://github.com/ceph/ceph/pull/43793>`_, Igor Fedotov)
* os/bluestore: list obj which equals to pend (`pr#43512 <https://github.com/ceph/ceph/pull/43512>`_, Mykola Golub, Kefu Chai)
* os/bluestore: multiple repair fixes (`pr#43731 <https://github.com/ceph/ceph/pull/43731>`_, Igor Fedotov)
* osd/OSD: mkfs need wait for transcation completely finish (`pr#43417 <https://github.com/ceph/ceph/pull/43417>`_, Chen Fan)
* osd: fix partial recovery become whole object recovery after restart osd (`pr#43513 <https://github.com/ceph/ceph/pull/43513>`_, Jianwei Zhang)
* osd: fix to allow inc manifest leaked (`pr#43306 <https://github.com/ceph/ceph/pull/43306>`_, Myoungwon Oh)
* osd: fix to recover adjacent clone when set_chunk is called (`pr#43099 <https://github.com/ceph/ceph/pull/43099>`_, Myoungwon Oh)
* osd: handle inconsistent hash info during backfill and deep scrub gracefully (`pr#43544 <https://github.com/ceph/ceph/pull/43544>`_, Ronen Friedman, Mykola Golub)
* osd: re-cache peer_bytes on every peering state activate (`pr#43437 <https://github.com/ceph/ceph/pull/43437>`_, Mykola Golub)
* osd: Run osd bench test to override default max osd capacity for mclock (`pr#41731 <https://github.com/ceph/ceph/pull/41731>`_, Sridhar Seshasayee)
* Pacific: BlueStore: Omap upgrade to per-pg fix fix (`pr#43922 <https://github.com/ceph/ceph/pull/43922>`_, Adam Kupczyk)
* Pacific: client: do not defer releasing caps when revoking (`pr#43782 <https://github.com/ceph/ceph/pull/43782>`_, Xiubo Li)
* Pacific: mds: add read/write io size metrics support (`pr#43784 <https://github.com/ceph/ceph/pull/43784>`_, Xiubo Li)
* Pacific: test/libcephfs: put inodes after lookup (`pr#43562 <https://github.com/ceph/ceph/pull/43562>`_, Patrick Donnelly)
* pybind/mgr/cephadm: set allow_standby_replay during CephFS upgrade (`pr#43559 <https://github.com/ceph/ceph/pull/43559>`_, Patrick Donnelly)
* pybind/mgr/CMakeLists.txt: exclude files not used at runtime (`pr#43787 <https://github.com/ceph/ceph/pull/43787>`_, Duncan Bellamy)
* pybind/mgr/pg_autoscale: revert to default profile scale-up (`pr#44032 <https://github.com/ceph/ceph/pull/44032>`_, Kamoltat)
* qa/mgr/dashboard/test_pool: don't check HEALTH_OK (`pr#43440 <https://github.com/ceph/ceph/pull/43440>`_, Ernesto Puerta)
* qa/mgr/dashboard: add extra wait to test (`pr#43351 <https://github.com/ceph/ceph/pull/43351>`_, Ernesto Puerta)
* qa/rgw: pacific branch targets ceph-pacific branch of java_s3tests (`pr#43809 <https://github.com/ceph/ceph/pull/43809>`_, Casey Bodley)
* qa/tasks/kubeadm: force docker cgroup engine to systemd (`pr#43937 <https://github.com/ceph/ceph/pull/43937>`_, Sage Weil)
* qa/tasks/mgr: skip test_diskprediction_local on python>=3.8 (`pr#43421 <https://github.com/ceph/ceph/pull/43421>`_, Kefu Chai)
* qa/tests: advanced version to reflect the latest 16.2.6 release (`pr#43242 <https://github.com/ceph/ceph/pull/43242>`_, Yuri Weinstein)
* qa: disable metrics on kernel client during upgrade (`pr#44034 <https://github.com/ceph/ceph/pull/44034>`_, Patrick Donnelly)
* qa: lengthen grace for fs map showing dead MDS (`pr#43702 <https://github.com/ceph/ceph/pull/43702>`_, Patrick Donnelly)
* qa: reduce frag split confs for dir_split counter test (`pr#43828 <https://github.com/ceph/ceph/pull/43828>`_, Patrick Donnelly)
* rbd-mirror: fix mirror image removal (`pr#43662 <https://github.com/ceph/ceph/pull/43662>`_, Arthur Outhenin-Chalandre)
* rbd-mirror: unbreak one-way snapshot-based mirroring (`pr#43315 <https://github.com/ceph/ceph/pull/43315>`_, Ilya Dryomov)
* rgw/notification: make notifications agnostic of bucket reshard (`pr#42946 <https://github.com/ceph/ceph/pull/42946>`_, Yuval Lifshitz)
* rgw/notifications: cache object size to avoid accessing invalid memory (`pr#42949 <https://github.com/ceph/ceph/pull/42949>`_, Yuval Lifshitz)
* rgw/notifications: send correct size in case of delete marker creation (`pr#42643 <https://github.com/ceph/ceph/pull/42643>`_, Yuval Lifshitz)
* rgw/notifications: support v4 auth for topics and notifications (`pr#42947 <https://github.com/ceph/ceph/pull/42947>`_, Yuval Lifshitz)
* rgw/rgw_rados: make RGW request IDs non-deterministic (`pr#43695 <https://github.com/ceph/ceph/pull/43695>`_, Cory Snyder)
* rgw/sts: fix for copy object operation using sts (`pr#43703 <https://github.com/ceph/ceph/pull/43703>`_, Pritha Srivastava)
* rgw/tracing: unify SO version numbers within librgw2 package (`pr#43619 <https://github.com/ceph/ceph/pull/43619>`_, Nathan Cutler)
* rgw: add abstraction for ops log destination and add file logger (`pr#43740 <https://github.com/ceph/ceph/pull/43740>`_, Casey Bodley, Cory Snyder)
* rgw: Ensure buckets too old to decode a layout have layout logs (`pr#43823 <https://github.com/ceph/ceph/pull/43823>`_, Adam C. Emerson)
* rgw: fix bucket purge incomplete multipart uploads (`pr#43862 <https://github.com/ceph/ceph/pull/43862>`_, J. Eric Ivancich)
* rgw: fix spelling of eTag in S3 message structure (`pr#42945 <https://github.com/ceph/ceph/pull/42945>`_, Tom Schoonjans)
* rgw: fix sts memory leak (`pr#43348 <https://github.com/ceph/ceph/pull/43348>`_, yuliyang_yewu)
* rgw: remove prefix & delim params for bucket removal & mp upload abort (`pr#43975 <https://github.com/ceph/ceph/pull/43975>`_, J. Eric Ivancich)
* rgw: use existing s->bucket in s3 website retarget() (`pr#43777 <https://github.com/ceph/ceph/pull/43777>`_, Casey Bodley)
* snap-schedule: count retained snapshots per retention policy (`pr#43434 <https://github.com/ceph/ceph/pull/43434>`_, Jan Fajerski)
* test: shutdown the mounter after test finishes (`pr#43475 <https://github.com/ceph/ceph/pull/43475>`_, Xiubo Li)

v16.2.6 Pacific
===============

.. DANGER:: DATE: 01 NOV 2021. 

   DO NOT UPGRADE TO CEPH PACIFIC FROM AN OLDER VERSION.  

   A recently-discovered bug (https://tracker.ceph.com/issues/53062) can cause
   data corruption. This bug occurs during OMAP format conversion for
   clusters that are updated to Pacific. New clusters are not affected by this
   bug.

   The trigger for this bug is BlueStore's repair/quick-fix functionality. This
   bug can be triggered in two known ways: 

    (1) manually via the ceph-bluestore-tool, or 
    (2) automatically, by OSD if ``bluestore_fsck_quick_fix_on_mount`` is set 
        to true.

   The fix for this bug is expected to be available in Ceph v16.2.7.

   DO NOT set ``bluestore_quick_fix_on_mount`` to true. If it is currently
   set to true in your configuration, immediately set it to false.

   DO NOT run ``ceph-bluestore-tool``'s repair/quick-fix commands.


This is the sixth backport release in the Pacific series. 


Notable Changes
---------------

* MGR: The pg_autoscaler has a new default 'scale-down' profile which provides more
  performance from the start for new pools (for newly created clusters).
  Existing clusters will retain the old behavior, now called the 'scale-up' profile.
  For more details, see:
  https://docs.ceph.com/en/latest/rados/operations/placement-groups/

* CephFS: the upgrade procedure for CephFS is now simpler. It is no longer
  necessary to stop all MDS before upgrading the sole active MDS. After
  disabling standby-replay, reducing max_mds to 1, and waiting for the file
  systems to become stable (each fs with 1 active and 0 stopping daemons), a
  rolling upgrade of all MDS daemons can be performed.

* Dashboard: now allows users to set up and display a custom message (MOTD, warning,
  etc.) in a sticky banner at the top of the page. For more details, see:
  https://docs.ceph.com/en/pacific/mgr/dashboard/#message-of-the-day-motd

* Several fixes in BlueStore, including a fix for the deferred write regression,
  which led to excessive RocksDB flushes and compactions. Previously, when
  `bluestore_prefer_deferred_size_hdd` was equal to or more than
  `bluestore_max_blob_size_hdd` (both set to 64K), all the data was deferred,
  which led to increased consumption of the column family used to store
  deferred writes in RocksDB. Now, the `bluestore_prefer_deferred_size` parameter
  independently controls deferred writes, and only writes smaller than
  this size use the deferred write path.

* The default value of `osd_client_message_cap` has been set to 256, to provide
  better flow control by limiting maximum number of in-flight client requests.

* PGs no longer show a `active+clean+scrubbing+deep+repair` state when
  `osd_scrub_auto_repair` is set to true, for regular deep-scrubs with no repair
  required.

* `ceph-mgr-modules-core` debian package does not recommend `ceph-mgr-rook`
  anymore. As the latter depends on `python3-numpy` which cannot be imported in
  different Python sub-interpreters multi-times if the version of
  `python3-numpy` is older than 1.19. Since `apt-get` installs the `Recommends`
  packages by default, `ceph-mgr-rook` was always installed along with
  `ceph-mgr` debian package as an indirect dependency. If your workflow depends
  on this behavior, you might want to install `ceph-mgr-rook` separately.
  
 * This is the first release built for Debian Bullseye.

Changelog
---------

* bind on loopback address if no other addresses are available (`pr#42477 <https://github.com/ceph/ceph/pull/42477>`_, Kefu Chai)
* ceph-monstore-tool: use a large enough paxos/{first,last}_committed (`issue#38219 <http://tracker.ceph.com/issues/38219>`_, `pr#42411 <https://github.com/ceph/ceph/pull/42411>`_, Kefu Chai)
* ceph-volume/tests: retry when destroying osd (`pr#42546 <https://github.com/ceph/ceph/pull/42546>`_, Guillaume Abrioux)
* ceph-volume/tests: update ansible environment variables in tox (`pr#42490 <https://github.com/ceph/ceph/pull/42490>`_, Dimitri Savineau)
* ceph-volume: Consider /dev/root as mounted (`pr#42755 <https://github.com/ceph/ceph/pull/42755>`_, David Caro)
* ceph-volume: fix lvm activate arguments (`pr#43116 <https://github.com/ceph/ceph/pull/43116>`_, Dimitri Savineau)
* ceph-volume: fix lvm migrate without args (`pr#43110 <https://github.com/ceph/ceph/pull/43110>`_, Dimitri Savineau)
* ceph-volume: fix raw list with logical partition (`pr#43087 <https://github.com/ceph/ceph/pull/43087>`_, Guillaume Abrioux, Dimitri Savineau)
* ceph-volume: implement bluefs volume migration (`pr#42219 <https://github.com/ceph/ceph/pull/42219>`_, Kefu Chai, Igor Fedotov)
* ceph-volume: lvm batch: fast_allocations(): avoid ZeroDivisionError (`pr#42493 <https://github.com/ceph/ceph/pull/42493>`_, Jonas Zeiger)
* ceph-volume: pvs --noheadings replace pvs --no-heading (`pr#43076 <https://github.com/ceph/ceph/pull/43076>`_, FengJiankui)
* ceph-volume: remove --all ref from deactivate help (`pr#43098 <https://github.com/ceph/ceph/pull/43098>`_, Dimitri Savineau)
* ceph-volume: support no_systemd with lvm migrate (`pr#43091 <https://github.com/ceph/ceph/pull/43091>`_, Dimitri Savineau)
* ceph-volume: work around phantom atari partitions (`pr#42753 <https://github.com/ceph/ceph/pull/42753>`_, Blaine Gardner)
* ceph.spec.in: drop gdbm from build deps (`pr#43000 <https://github.com/ceph/ceph/pull/43000>`_, Kefu Chai)
* cephadm: August batch 1 (`pr#42736 <https://github.com/ceph/ceph/pull/42736>`_, Sage Weil, Dimitri Savineau, Guillaume Abrioux, Sebastian Wagner, Varsha Rao, Zac Dover, Adam King, Cory Snyder, Michael Fritch, Asbjørn Sannes, "Wang,Fei", Javier Cacheiro, 胡玮文, Daniel Pivonka)
* cephadm: September batch 1 (`issue#52038 <http://tracker.ceph.com/issues/52038>`_, `pr#43029 <https://github.com/ceph/ceph/pull/43029>`_, Sebastian Wagner, Dimitri Savineau, Paul Cuzner, Oleander Reis, Adam King, Yuxiang Zhu, Zac Dover, Alfonso Martínez, Sage Weil, Daniel Pivonka)
* cephadm: use quay, not docker (`pr#42534 <https://github.com/ceph/ceph/pull/42534>`_, Sage Weil)
* cephfs-mirror: record directory path cancel in DirRegistry (`issue#51666 <http://tracker.ceph.com/issues/51666>`_, `pr#42458 <https://github.com/ceph/ceph/pull/42458>`_, Venky Shankar)
* client: flush the mdlog in unsafe requests' relevant and auth MDSes only (`pr#42925 <https://github.com/ceph/ceph/pull/42925>`_, Xiubo Li)
* client: make sure only to update dir dist from auth mds (`pr#42937 <https://github.com/ceph/ceph/pull/42937>`_, Xue Yantao)
* cls/cmpomap: empty values are 0 in U64 comparisons (`pr#42908 <https://github.com/ceph/ceph/pull/42908>`_, Casey Bodley)
* cmake, ceph.spec.in: build with header only fmt on RHEL (`pr#42472 <https://github.com/ceph/ceph/pull/42472>`_, Kefu Chai)
* cmake: build static libs if they are internal ones (`pr#39902 <https://github.com/ceph/ceph/pull/39902>`_, Kefu Chai)
* cmake: exclude "grafonnet-lib" target from "all" (`pr#42898 <https://github.com/ceph/ceph/pull/42898>`_, Kefu Chai)
* cmake: link bundled fmt statically (`pr#42692 <https://github.com/ceph/ceph/pull/42692>`_, Kefu Chai)
* cmake: Replace boost download url (`pr#42693 <https://github.com/ceph/ceph/pull/42693>`_, Rafał Wądołowski)
* common/buffer: fix SIGABRT in  rebuild_aligned_size_and_memory (`pr#42976 <https://github.com/ceph/ceph/pull/42976>`_, Yin Congmin)
* common/Formatter: include used header (`pr#42233 <https://github.com/ceph/ceph/pull/42233>`_, Kefu Chai)
* common/options: Set osd_client_message_cap to 256 (`pr#42615 <https://github.com/ceph/ceph/pull/42615>`_, Mark Nelson)
* compression/snappy: use uint32_t to be compatible with 1.1.9 (`pr#42542 <https://github.com/ceph/ceph/pull/42542>`_, Kefu Chai, Nathan Cutler)
* debian/control: ceph-mgr-modules-core does not Recommend ceph-mgr-roo… (`pr#42300 <https://github.com/ceph/ceph/pull/42300>`_, Kefu Chai)
* debian/control: dh-systemd is part of debhelper now (`pr#43151 <https://github.com/ceph/ceph/pull/43151>`_, David Galloway)
* debian/control: remove cython from Build-Depends (`pr#43131 <https://github.com/ceph/ceph/pull/43131>`_, Kefu Chai)
* doc/ceph-volume: add lvm migrate/new-db/new-wal (`pr#43089 <https://github.com/ceph/ceph/pull/43089>`_, Dimitri Savineau)
* doc/rados/operations: s/max_misplaced/target_max_misplaced_ratio/ (`pr#42250 <https://github.com/ceph/ceph/pull/42250>`_, Paul Reece, Kefu Chai)
* doc/releases/pacific.rst: remove notes about autoscaler (`pr#42265 <https://github.com/ceph/ceph/pull/42265>`_, Neha Ojha)
* Don't persist report data (`pr#42888 <https://github.com/ceph/ceph/pull/42888>`_, Brad Hubbard)
* krbd: escape udev_enumerate_add_match_sysattr values (`pr#42969 <https://github.com/ceph/ceph/pull/42969>`_, Ilya Dryomov)
* kv/RocksDBStore: Add handling of block_cache option for resharding (`pr#42844 <https://github.com/ceph/ceph/pull/42844>`_, Adam Kupczyk)
* kv/RocksDBStore: enrich debug message (`pr#42544 <https://github.com/ceph/ceph/pull/42544>`_, Toshikuni Fukaya, Satoru Takeuchi)
* librgw/notifications: initialize kafka and amqp (`pr#42648 <https://github.com/ceph/ceph/pull/42648>`_, Yuval Lifshitz)
* mds: add debugging when rejecting mksnap with EPERM (`pr#42935 <https://github.com/ceph/ceph/pull/42935>`_, Patrick Donnelly)
* mds: create file system with specific ID (`pr#42900 <https://github.com/ceph/ceph/pull/42900>`_, Ramana Raja)
* mds: MDCache.cc:5319 FAILED ceph_assert(rejoin_ack_gather.count(mds->get_nodeid())) (`pr#42938 <https://github.com/ceph/ceph/pull/42938>`_, chencan)
* mds: META_POP_READDIR, META_POP_FETCH, META_POP_STORE, and cache_hit_rate are not updated (`pr#42939 <https://github.com/ceph/ceph/pull/42939>`_, Yongseok Oh)
* mds: to print the unknow type value (`pr#42088 <https://github.com/ceph/ceph/pull/42088>`_, Xiubo Li, Jos Collin)
* MDSMonitor: monitor crash after upgrade from ceph 15.2.13 to 16.2.4 (`pr#42536 <https://github.com/ceph/ceph/pull/42536>`_, Patrick Donnelly)
* mgr/DaemonServer: skip redundant update of pgp_num_actual (`pr#42223 <https://github.com/ceph/ceph/pull/42223>`_, Dan van der Ster)
* mgr/dashboard/api: set a UTF-8 locale when running pip (`pr#42829 <https://github.com/ceph/ceph/pull/42829>`_, Kefu Chai)
* mgr/dashboard: Add configurable MOTD or wall notification (`pr#42414 <https://github.com/ceph/ceph/pull/42414>`_, Volker Theile)
* mgr/dashboard: cephadm e2e start script: add --expanded option (`pr#42789 <https://github.com/ceph/ceph/pull/42789>`_, Alfonso Martínez)
* mgr/dashboard: cephadm-e2e job script: improvements (`pr#42585 <https://github.com/ceph/ceph/pull/42585>`_, Alfonso Martínez)
* mgr/dashboard: disable create snapshot with subvolumes (`pr#42819 <https://github.com/ceph/ceph/pull/42819>`_, Pere Diaz Bou)
* mgr/dashboard: don't notify for suppressed alerts (`pr#42974 <https://github.com/ceph/ceph/pull/42974>`_, Tatjana Dehler)
* mgr/dashboard: fix Accept-Language header parsing (`pr#42297 <https://github.com/ceph/ceph/pull/42297>`_, 胡玮文)
* mgr/dashboard: fix rename inventory to disks (`pr#42810 <https://github.com/ceph/ceph/pull/42810>`_, Navin Barnwal)
* mgr/dashboard: fix ssl cert validation for rgw service creation (`pr#42628 <https://github.com/ceph/ceph/pull/42628>`_, Avan Thakkar)
* mgr/dashboard: Fix test_error force maintenance dashboard check (`pr#42354 <https://github.com/ceph/ceph/pull/42354>`_, Nizamudeen A)
* mgr/dashboard: monitoring: replace Grafana JSON with Grafonnet based code (`pr#42812 <https://github.com/ceph/ceph/pull/42812>`_, Aashish Sharma)
* mgr/dashboard: Refresh button on the iscsi targets page (`pr#42817 <https://github.com/ceph/ceph/pull/42817>`_, Nizamudeen A)
* mgr/dashboard: remove usage of 'rgw_frontend_ssl_key' (`pr#42316 <https://github.com/ceph/ceph/pull/42316>`_, Avan Thakkar)
* mgr/dashboard: show perf. counters for rgw svc. on Cluster > Hosts (`pr#42629 <https://github.com/ceph/ceph/pull/42629>`_, Alfonso Martínez)
* mgr/dashboard: stats=false not working when listing buckets (`pr#42889 <https://github.com/ceph/ceph/pull/42889>`_, Avan Thakkar)
* mgr/dashboard: tox.ini: delete useless env. 'apidocs' (`pr#42788 <https://github.com/ceph/ceph/pull/42788>`_, Alfonso Martínez)
* mgr/dashboard: update translations for pacific (`pr#42606 <https://github.com/ceph/ceph/pull/42606>`_, Tatjana Dehler)
* mgr/mgr_util: switch using unshared cephfs connections whenever possible (`issue#51256 <http://tracker.ceph.com/issues/51256>`_, `pr#42083 <https://github.com/ceph/ceph/pull/42083>`_, Venky Shankar)
* mgr/pg_autoscaler: Introduce autoscaler scale-down feature (`pr#42428 <https://github.com/ceph/ceph/pull/42428>`_, Kamoltat, Kefu Chai)
* mgr/rook: Add timezone info (`pr#39834 <https://github.com/ceph/ceph/pull/39834>`_, Varsha Rao, Sebastian Wagner)
* mgr/telemetry: pass leaderboard flag even w/o ident (`pr#42228 <https://github.com/ceph/ceph/pull/42228>`_, Sage Weil)
* mgr/volumes: Add config to insert delay at the beginning of the clone (`pr#42086 <https://github.com/ceph/ceph/pull/42086>`_, Kotresh HR)
* mgr/volumes: use dedicated libcephfs handles for subvolume calls and … (`issue#51271 <http://tracker.ceph.com/issues/51271>`_, `pr#42914 <https://github.com/ceph/ceph/pull/42914>`_, Venky Shankar)
* mgr: set debug_mgr=2/5 (so INFO goes to mgr log by default) (`pr#42225 <https://github.com/ceph/ceph/pull/42225>`_, Sage Weil)
* mon/MDSMonitor: do not pointlessly kill standbys that are incompatible with current CompatSet (`pr#42578 <https://github.com/ceph/ceph/pull/42578>`_, Patrick Donnelly, Zhi Zhang)
* mon/OSDMonitor: resize oversized Lec::epoch_by_pg, after PG merging, preventing osdmap trimming (`pr#42224 <https://github.com/ceph/ceph/pull/42224>`_, Dan van der Ster)
* mon/PGMap: remove DIRTY field in ceph df detail when cache tiering is not in use (`pr#42860 <https://github.com/ceph/ceph/pull/42860>`_, Deepika Upadhyay)
* mon: return -EINVAL when handling unknown option in 'ceph osd pool get' (`pr#42229 <https://github.com/ceph/ceph/pull/42229>`_, Zhao Cuicui)
* mon: Sanely set the default CRUSH rule when creating pools in stretch… (`pr#42909 <https://github.com/ceph/ceph/pull/42909>`_, Greg Farnum)
* monitoring/grafana/build/Makefile: revamp for arm64 builds, pushes to docker and quay, jenkins (`pr#42211 <https://github.com/ceph/ceph/pull/42211>`_, Dan Mick)
* monitoring/grafana/cluster: use per-unit max and limit values (`pr#42679 <https://github.com/ceph/ceph/pull/42679>`_, David Caro)
* monitoring: Clean up Grafana dashboards (`pr#42299 <https://github.com/ceph/ceph/pull/42299>`_, Patrick Seidensal)
* monitoring: fix Physical Device Latency unit (`pr#42298 <https://github.com/ceph/ceph/pull/42298>`_, Seena Fallah)
* msg: active_connections regression (`pr#42936 <https://github.com/ceph/ceph/pull/42936>`_, Sage Weil)
* nfs backport June (`pr#42096 <https://github.com/ceph/ceph/pull/42096>`_, Varsha Rao)
* os/bluestore: accept undecodable multi-block bluefs transactions on log (`pr#43023 <https://github.com/ceph/ceph/pull/43023>`_, Igor Fedotov)
* os/bluestore: cap omap naming scheme upgrade transaction (`pr#42956 <https://github.com/ceph/ceph/pull/42956>`_, Igor Fedotov)
* os/bluestore: compact db after bulk omap naming upgrade (`pr#42426 <https://github.com/ceph/ceph/pull/42426>`_, Igor Fedotov)
* os/bluestore: fix bluefs migrate command (`pr#43100 <https://github.com/ceph/ceph/pull/43100>`_, Igor Fedotov)
* os/bluestore: fix erroneous SharedBlob record removal during repair (`pr#42423 <https://github.com/ceph/ceph/pull/42423>`_, Igor Fedotov)
* os/bluestore: fix using incomplete bluefs log when dumping it (`pr#43007 <https://github.com/ceph/ceph/pull/43007>`_, Igor Fedotov)
* os/bluestore: make deferred writes less aggressive for large writes (`pr#42773 <https://github.com/ceph/ceph/pull/42773>`_, Igor Fedotov, Adam Kupczyk)
* os/bluestore: Remove possibility of replay log and file inconsistency (`pr#42424 <https://github.com/ceph/ceph/pull/42424>`_, Adam Kupczyk)
* os/bluestore: respect bluestore_warn_on_spurious_read_errors setting (`pr#42897 <https://github.com/ceph/ceph/pull/42897>`_, Igor Fedotov)
* osd/scrub: separate between PG state flags and internal scrubber operation (`pr#42398 <https://github.com/ceph/ceph/pull/42398>`_, Ronen Friedman)
* osd: log snaptrim message to dout (`pr#42482 <https://github.com/ceph/ceph/pull/42482>`_, Arthur Outhenin-Chalandre)
* osd: move down peers out from peer_purged (`pr#42238 <https://github.com/ceph/ceph/pull/42238>`_, Mykola Golub)
* pybind/mgr/stats: validate cmdtag (`pr#42702 <https://github.com/ceph/ceph/pull/42702>`_, Jos Collin)
* pybind/mgr: Fix IPv6 url generation (`pr#42990 <https://github.com/ceph/ceph/pull/42990>`_, Sebastian Wagner)
* pybind/rbd: fix mirror_image_get_status (`pr#42972 <https://github.com/ceph/ceph/pull/42972>`_, Ilya Dryomov, Will Smith)
* qa/\*/test_envlibrados_for_rocksdb.sh: install libarchive-3.3.3 (`pr#42344 <https://github.com/ceph/ceph/pull/42344>`_, Neha Ojha)
* qa/cephadm: centos_8.x_container_tools_3.0.yaml (`pr#42868 <https://github.com/ceph/ceph/pull/42868>`_, Sebastian Wagner)
* qa/rgw: move ignore-pg-availability.yaml out of suites/rgw (`pr#40694 <https://github.com/ceph/ceph/pull/40694>`_, Casey Bodley)
* qa/standalone: Add missing cleanups after completion of a subset of osd and scrub tests (`pr#42258 <https://github.com/ceph/ceph/pull/42258>`_, Sridhar Seshasayee)
* qa/tests: advanced pacific version to reflect the latest 16.2.5 point (`pr#42264 <https://github.com/ceph/ceph/pull/42264>`_, Yuri Weinstein)
* qa/workunits/mon/test_mon_config_key: use subprocess.run() instead of proc.communicate() (`pr#42221 <https://github.com/ceph/ceph/pull/42221>`_, Kefu Chai)
* qa: FileNotFoundError: [Errno 2] No such file or directory: '/sys/kernel/debug/ceph/3fab6bea-f243-47a4-a956-8c03a62b61b5.client4721/mds_sessions' (`pr#42165 <https://github.com/ceph/ceph/pull/42165>`_, Patrick Donnelly)
* qa: increase the pg_num for cephfs_data/metadata pools (`pr#42923 <https://github.com/ceph/ceph/pull/42923>`_, Xiubo Li)
* qa: test_ls_H_prints_human_readable_file_size failure (`pr#42166 <https://github.com/ceph/ceph/pull/42166>`_, Patrick Donnelly)
* radosgw-admin: skip GC init on read-only admin ops (`pr#42655 <https://github.com/ceph/ceph/pull/42655>`_, Mark Kogan)
* radosgw: include realm\_{id,name} in service map (`pr#42213 <https://github.com/ceph/ceph/pull/42213>`_, Sage Weil)
* rbd-mirror: add perf counters to snapshot replayer (`pr#42987 <https://github.com/ceph/ceph/pull/42987>`_, Arthur Outhenin-Chalandre)
* rbd-mirror: fix potential async op tracker leak in start_image_replayers (`pr#42979 <https://github.com/ceph/ceph/pull/42979>`_, Mykola Golub)
* rbd: fix default pool handling for nbd map/unmap (`pr#42980 <https://github.com/ceph/ceph/pull/42980>`_, Sunny Kumar)
* Remove dependency on lsb_release (`pr#43001 <https://github.com/ceph/ceph/pull/43001>`_, Ken Dreyer)
* RGW - Bucket Remove Op: Pass in user (`pr#42135 <https://github.com/ceph/ceph/pull/42135>`_, Daniel Gryniewicz)
* RGW - Don't move attrs before setting them (`pr#42320 <https://github.com/ceph/ceph/pull/42320>`_, Daniel Gryniewicz)
* rgw : add check empty for sync url (`pr#42653 <https://github.com/ceph/ceph/pull/42653>`_, caolei)
* rgw : add check for tenant provided in RGWCreateRole (`pr#42637 <https://github.com/ceph/ceph/pull/42637>`_, caolei)
* rgw : modfiy error XML for deleterole (`pr#42639 <https://github.com/ceph/ceph/pull/42639>`_, caolei)
* rgw multisite: metadata sync treats all errors as 'transient' for retry (`pr#42656 <https://github.com/ceph/ceph/pull/42656>`_, Casey Bodley)
* RGW Zipper - Make sure bucket list progresses (`pr#42625 <https://github.com/ceph/ceph/pull/42625>`_, Daniel Gryniewicz)
* rgw/amqp/test: fix mock prototype for librabbitmq-0.11.0 (`pr#42649 <https://github.com/ceph/ceph/pull/42649>`_, Yuval Lifshitz)
* rgw/http/notifications: support content type in HTTP POST messages (`pr#42644 <https://github.com/ceph/ceph/pull/42644>`_, Yuval Lifshitz)
* rgw/multisite: return correct error code when op fails (`pr#42646 <https://github.com/ceph/ceph/pull/42646>`_, Yuval Lifshitz)
* rgw/notification: add exception handling for persistent notification thread (`pr#42647 <https://github.com/ceph/ceph/pull/42647>`_, Yuval Lifshitz)
* rgw/notification: fix persistent notification hang when ack-levl=none (`pr#40696 <https://github.com/ceph/ceph/pull/40696>`_, Yuval Lifshitz)
* rgw/notification: fixing the "persistent=false" flag (`pr#40695 <https://github.com/ceph/ceph/pull/40695>`_, Yuval Lifshitz)
* rgw/notifications: delete bucket notification object when empty (`pr#42631 <https://github.com/ceph/ceph/pull/42631>`_, Yuval Lifshitz)
* rgw/notifications: support metadata filter in CompleteMultipartUpload and Copy events (`pr#42321 <https://github.com/ceph/ceph/pull/42321>`_, Yuval Lifshitz)
* rgw/notifications: support metadata filter in CompleteMultipartUploa… (`pr#42566 <https://github.com/ceph/ceph/pull/42566>`_, Yuval Lifshitz)
* rgw/rgw_file: Fix the return value of read() and readlink() (`pr#42654 <https://github.com/ceph/ceph/pull/42654>`_, Dai zhiwei, luo rixin)
* rgw/sts: correcting the evaluation of session policies (`pr#42632 <https://github.com/ceph/ceph/pull/42632>`_, Pritha Srivastava)
* rgw/sts: read_obj_policy() consults iam_user_policies on ENOENT (`pr#42650 <https://github.com/ceph/ceph/pull/42650>`_, Casey Bodley)
* rgw: allow rgw-orphan-list to process multiple data pools (`pr#42635 <https://github.com/ceph/ceph/pull/42635>`_, J. Eric Ivancich)
* rgw: allow to set ssl options and ciphers for beast frontend (`pr#42363 <https://github.com/ceph/ceph/pull/42363>`_, Mykola Golub)
* rgw: avoid infinite loop when deleting a bucket (`issue#49206 <http://tracker.ceph.com/issues/49206>`_, `pr#42230 <https://github.com/ceph/ceph/pull/42230>`_, Jeegn Chen)
* rgw: avoid occuring radosgw daemon crash when access a conditionally … (`pr#42626 <https://github.com/ceph/ceph/pull/42626>`_, xiangrui meng, yupeng chen)
* rgw: Backport of 51674 to Pacific (`pr#42346 <https://github.com/ceph/ceph/pull/42346>`_, Adam C. Emerson)
* rgw: deprecate the civetweb frontend (`pr#41367 <https://github.com/ceph/ceph/pull/41367>`_, Casey Bodley)
* rgw: Don't segfault on datalog trim (`pr#42336 <https://github.com/ceph/ceph/pull/42336>`_, Adam C. Emerson)
* rgw: during reshard lock contention, adjust logging (`pr#42641 <https://github.com/ceph/ceph/pull/42641>`_, J. Eric Ivancich)
* rgw: extending existing ssl support for vault KMS (`pr#42093 <https://github.com/ceph/ceph/pull/42093>`_, Jiffin Tony Thottan)
* rgw: fail as expected when set/delete-bucket-website attempted on a non-exis… (`pr#42642 <https://github.com/ceph/ceph/pull/42642>`_, xiangrui meng)
* rgw: fix bucket object listing when marker matches prefix (`pr#42638 <https://github.com/ceph/ceph/pull/42638>`_, J. Eric Ivancich)
* rgw: fix for mfa resync crash when supplied with only one totp_pin (`pr#42652 <https://github.com/ceph/ceph/pull/42652>`_, Pritha Srivastava)
* rgw: fix segfault related to explicit object manifest handling (`pr#42633 <https://github.com/ceph/ceph/pull/42633>`_, Mark Kogan)
* rgw: Improve error message on email id reuse (`pr#41783 <https://github.com/ceph/ceph/pull/41783>`_, Ponnuvel Palaniyappan)
* rgw: objectlock: improve client error messages (`pr#40693 <https://github.com/ceph/ceph/pull/40693>`_, Matt Benjamin)
* rgw: parse tenant name out of rgwx-bucket-instance (`pr#42231 <https://github.com/ceph/ceph/pull/42231>`_, Casey Bodley)
* rgw: radosgw-admin errors if marker not specified on data/mdlog trim (`pr#42640 <https://github.com/ceph/ceph/pull/42640>`_, Adam C. Emerson)
* rgw: remove quota soft threshold (`pr#42634 <https://github.com/ceph/ceph/pull/42634>`_, Zulai Wang)
* rgw: require bucket name in bucket chown (`pr#42323 <https://github.com/ceph/ceph/pull/42323>`_, Zulai Wang)
* rgw: when deleted obj removed in versioned bucket, extra del-marker added (`pr#42645 <https://github.com/ceph/ceph/pull/42645>`_, J. Eric Ivancich)
* rpm/luarocks: simplify conditional and support Leap 15.3 (`pr#42561 <https://github.com/ceph/ceph/pull/42561>`_, Nathan Cutler)
* rpm: drop use of $FIRST_ARG in ceph-immutable-object-cache (`pr#42480 <https://github.com/ceph/ceph/pull/42480>`_, Nathan Cutler)
* run-make-check.sh: Increase failure output log size (`pr#42850 <https://github.com/ceph/ceph/pull/42850>`_, David Galloway)
* SimpleRADOSStriper: use debug_cephsqlite (`pr#42659 <https://github.com/ceph/ceph/pull/42659>`_, Patrick Donnelly)
* src/pybind/mgr/mirroring/fs/snapshot_mirror.py: do not assume a cephf… (`pr#42226 <https://github.com/ceph/ceph/pull/42226>`_, Sébastien Han)
* test/rgw: fix use of poll() with timers in unittest_rgw_dmclock_scheduler (`pr#42651 <https://github.com/ceph/ceph/pull/42651>`_, Casey Bodley)
* Warning Cleanup and Clang Compile Fix (`pr#40692 <https://github.com/ceph/ceph/pull/40692>`_, Adam C. Emerson)
* workunits/rgw: semicolon terminates perl statements (`pr#43168 <https://github.com/ceph/ceph/pull/43168>`_, Matt Benjamin)

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
  For more information, see :ref:`mgr-nfs`.

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


.. _upgrading_from_octopus_or_nautilus:

Upgrading from Octopus or Nautilus
----------------------------------

Before starting, make sure your cluster is stable and healthy (no down or
recovering OSDs).  (This is optional, but recommended.)


.. note::
  WARNING: Please do not set `bluestore_fsck_quick_fix_on_mount` to true or
  run `ceph-bluestore-tool` repair or quick-fix commands in Pacific versions
  <= 16.2.6, because this can lead to data corruption, details in
  https://tracker.ceph.com/issues/53062.

.. note::
   When using multiple active Ceph Metadata Servers, ensure that there are
   no pending stray entries which are directories for active ranks except rank 0 as
   starting an upgrade (which sets `max_mds` to 1) could crash the Ceph
   Metadata Server. The following command should return zero (0) stray entries
   for all stray directories::

     # for idx in {0..9}; do ceph tell mds.<rank> dump tree ~mdsdir/stray$idx| jq '.[] | select (.nlink == 0 and .dir_layout.dir_hash > 0) | .stray_prior_path' | wc -l; done

   Ensure that all active ranks except rank 0 are checked for absence of stray
   entries which are directories (using the above command). Details are captured
   in http://tracker.ceph.com/issues/53597.

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

   #. Disable FSMap sanity checks::

        # ceph config set mon mon_mds_skip_sanity true

   #. Disable standby_replay::

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

   #. Remove `mon_mds_skip_sanity` setting::

        # ceph config rm mon mon_mds_skip_sanity

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

