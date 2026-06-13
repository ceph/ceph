========
Tentacle
========

Tentacle is the 20th stable release of Ceph.

v20.2.1 Tentacle
================

This is the first minor release in the Tentacle series. We recommend that all users update to this release.

Release Date
------------

April 06, 2026

Notable Changes
---------------

OSD / BlueStore
---------------

* EC Recovery: Fixed a length calculation bug in erase_after_ro_offset() that caused empty shards to retain data, leading to shard_size >= tobj_size assertion failures when recovering small objects in EC pools.
* BlueFS Volume Selector: Updated the BlueFS volume selector to properly account for file size changes when recovering the WAL in envelope mode.
* BlueFS: Fixed a bug where stat() missed the actual file size update after indexing WAL envelope files.

Monitor (mon)
-------------

* Fast EC Restrictions: Denied the ability to enable EC optimizations ("fast EC") for non-4K-aligned chunk sizes. Unaligned chunk sizes handled by fast EC perform poorly and suffer from bugs, so attempts to force this configuration are now rejected.
* Peering: Ensured ceph pg repeer proposes a correctly sized pg temp, as optimized EC cannot cope with mismatched sizes.
* NVMeoF Gateway: Added a new nvme-gw listeners command to display all existing listeners (including auto-listeners) inside a pool/group.
* NVMeoF Failover: Overhauled the NVMeoF Gateway fast-failover logic. Beacon timeouts are now evaluated within prepare_beacon to support shorter intervals, and the mechanism for detecting monitor slowness was improved.

librbd & rbd-mirror
-------------------

* RBD: Introduced a new ``RBD_LOCK_MODE_EXCLUSIVE_TRANSIENT`` policy for ``rbd_lock_acquire()``. This is a low-level interface intended to allow a peer to grab exclusive lock manually for short periods of time with other peers pausing their activity and waiting for the lock to be released rather than instantly aborting I/O and returning an error. It's possible to switch from ``RBD_LOCK_MODE_EXCLUSIVE`` to ``RBD_LOCK_MODE_EXCLUSIVE_TRANSIENT`` policy and vice versa even if the lock is already held.

Ceph Object Gateway (RGW)
-------------------------

* Multi-Part Operations: Fixed conditional validation handling in MultiWrite, Delete, and MultiDelete workflows.

mgr/dashboard
-------------

* UI Navigation: Redesigned the main landing page; the "Dashboard" navigation item was renamed to "Overview" and uses a new carbonized productive card layout.
* NVMeoF Management: Added the nvmeof get_subsystems CLI command, fixed JSON output indentation for NVMeoF CLI commands, and reverted the server_addr API parameter back to traddr for consistency.
* Hosts View: Fixed a bug causing the IP addresses of hosts to be hidden on the Hosts page due to an issue with fact merging.
* Forms & Modals: Standardized forms onto the Carbon Design System, including the pools form, service form, multi-site realm token export modal, delete zone modal, and password change forms.
* Form Validation: Generalized form error handling and validations using a new cdValidate directive.

mgr/cephadm
-----------

* Monitoring Stack: Bumped the default container image versions for the monitoring stack: Prometheus to v3.6.0, Node-exporter to v1.9.1, Alertmanager to v0.28.1, and Grafana to v12.2.0.

Security Changes
----------------

* Monitoring Stack Images: Updated Prometheus, Alertmanager, and Grafana container image versions, picking up upstream security and stability fixes.

Configuration Changes
---------------------

* ``bluefs_check_volume_selector_on_mount``: The previous bluefs_check_volume_selector_on_umount debug setting was renamed and repurposed. It now checks for volume selector inconsistencies on both mount and unmount phases.

* ``mon_nvmeofgw_beacon_grace``: The default grace period before marking a gateway as failed has been reduced from 10 seconds to 7 seconds for faster failover.

* ``nvmeof_mon_client_tick_period``: The default beacon tick interval has been lowered from 2 seconds to 1 second.


Changelog
---------

* [rgw][tentacle] backport of cloud-restore related PRs (`pr#65830 <https://github.com/ceph/ceph/pull/65830>`_, Soumya Koduri)
* Add normalization and casesensitive options to the subvolume group creation command (`pr#65564 <https://github.com/ceph/ceph/pull/65564>`_, Venky Shankar, Xavi Hernandez)
* auth: msgr2 can return incorrect allowed_modes through AuthBadMethodFrame (`pr#65336 <https://github.com/ceph/ceph/pull/65336>`_, Miki Patel)
* backports variants improvements and Dockerfile.build changes (`pr#66010 <https://github.com/ceph/ceph/pull/66010>`_, John Mulligan, Zack Cerza)
* Beacon diff (`pr#66958 <https://github.com/ceph/ceph/pull/66958>`_, Leonid Chernin, Samuel Just)
* blk/kernel: bring "bdev_async_discard" config parameter back (`pr#65609 <https://github.com/ceph/ceph/pull/65609>`_, Igor Fedotov)
* blk/kernel: improve DiscardThread life cycle (`pr#65213 <https://github.com/ceph/ceph/pull/65213>`_, Igor Fedotov)
* bluestore/BlueFS: fix bytes_written_slow counter with aio_write (`pr#66355 <https://github.com/ceph/ceph/pull/66355>`_, chungfengz)
* build-with-container: add argument groups to organize options (`pr#65628 <https://github.com/ceph/ceph/pull/65628>`_, John Mulligan)
* build-with-container: build image variants (`pr#65946 <https://github.com/ceph/ceph/pull/65946>`_, John Mulligan)
* ceph-mixin: Update monitoring mixin (`pr#65692 <https://github.com/ceph/ceph/pull/65692>`_, Aashish Sharma, SuperQ, Ankush Behl)
* ceph-volume: fix UdevData initialisation from empty /run/udev/data/\* file (`pr#65923 <https://github.com/ceph/ceph/pull/65923>`_, Matteo Paramatti)
* ceph-volume: lvm.Lvm.setup_metadata_devices refactor (`pr#65925 <https://github.com/ceph/ceph/pull/65925>`_, Guillaume Abrioux)
* ceph-volume: support additional dmcrypt params (`pr#65544 <https://github.com/ceph/ceph/pull/65544>`_, Guillaume Abrioux)
* ceph-volume: use udev data instead of LVM subprocess in get_devices() (`pr#65921 <https://github.com/ceph/ceph/pull/65921>`_, Guillaume Abrioux)
* ceph_release, doc/dev: update tentacle as stable release (`pr#65988 <https://github.com/ceph/ceph/pull/65988>`_, Laura Flores)
* cephadm, debian/rules: Use system packages for cephadm bundled dependencies (`pr#66256 <https://github.com/ceph/ceph/pull/66256>`_, Kefu Chai)
* cephadm: fix building rpm-sourced cephadm zippapp on el10 (`pr#65292 <https://github.com/ceph/ceph/pull/65292>`_, John Mulligan)
* cephadm: set default image for tentacle release (`pr#65719 <https://github.com/ceph/ceph/pull/65719>`_, Adam King)
* cephadm: support custom distros by falling back to ID_LIKE (`pr#65696 <https://github.com/ceph/ceph/pull/65696>`_, bachmanity1)
* cephfs-journal-tool: Journal trimming issue (`pr#65601 <https://github.com/ceph/ceph/pull/65601>`_, Kotresh HR)
* client: fix async/sync I/O stalling due to buffer list exceeding INT_MAX (`pr#65256 <https://github.com/ceph/ceph/pull/65256>`_, Dhairya Parmar)
* client: fix dump_mds_requests to valid json format (`issue#73639 <http://tracker.ceph.com/issues/73639>`_, `pr#66156 <https://github.com/ceph/ceph/pull/66156>`_, haoyixing)
* client: fix unmount hang after lookups (`pr#65254 <https://github.com/ceph/ceph/pull/65254>`_, Dhairya Parmar)
* client: use path supplied in statfs (`pr#65132 <https://github.com/ceph/ceph/pull/65132>`_, Christopher Hoffman)
* common/frag: properly convert frag_t to net/store endianness (`pr#66540 <https://github.com/ceph/ceph/pull/66540>`_, Patrick Donnelly, Max Kellermann)
* common: Allow PerfCounters to return a provided service ID (`pr#65587 <https://github.com/ceph/ceph/pull/65587>`_, Adam C. Emerson)
* debian/control: add iproute2 to build dependencies (`pr#66737 <https://github.com/ceph/ceph/pull/66737>`_, Kefu Chai)
* debian/control: Add libxsimd-dev build dependency for vendored Arrow (`pr#66248 <https://github.com/ceph/ceph/pull/66248>`_, Kefu Chai)
* debian/control: record python3-packaging dependency for ceph-volume (`pr#66590 <https://github.com/ceph/ceph/pull/66590>`_, Thomas Lamprecht, Max R. Carrara)
* doc/cephfs: fix docs for pause_purging and pause_cloning (`pr#66452 <https://github.com/ceph/ceph/pull/66452>`_, Rishabh Dave)
* doc/mgr/smb: document the 'provider' option for smb share (`pr#65617 <https://github.com/ceph/ceph/pull/65617>`_, Sachin Prabhu)
* doc/radosgw: change all intra-docs links to use ref (1 of 6) (`pr#67043 <https://github.com/ceph/ceph/pull/67043>`_, Ville Ojamo)
* doc/radosgw: change all intra-docs links to use ref (2 of 6) (`pr#67084 <https://github.com/ceph/ceph/pull/67084>`_, Ville Ojamo)
* doc/radosgw: Cosmetic improvements and ref links in account.rst (`pr#67064 <https://github.com/ceph/ceph/pull/67064>`_, Ville Ojamo)
* doc/rbd/rbd-config-ref: add clone settings section (`pr#66175 <https://github.com/ceph/ceph/pull/66175>`_, Ilya Dryomov)
* doc: add Tentacle to os recommendations (`pr#66464 <https://github.com/ceph/ceph/pull/66464>`_, Casey Bodley, Joseph Mundackal)
* doc: fetch releases from main branch (`pr#67002 <https://github.com/ceph/ceph/pull/67002>`_, Patrick Donnelly)
* doc: Pin pip to <25.3 for RTD as a workaround for pybind in admin/doc-read-the-docs.txt (`pr#66106 <https://github.com/ceph/ceph/pull/66106>`_, Ville Ojamo)
* doc: Remove sphinxcontrib-seqdiag Python package from RTD builds (`pr#67296 <https://github.com/ceph/ceph/pull/67296>`_, Ville Ojamo)
* doc: Update dashboard pending release notes (`pr#65984 <https://github.com/ceph/ceph/pull/65984>`_, Afreen Misbah)
* encode: Fix bad use of DENC_DUMP_PRE (`pr#66565 <https://github.com/ceph/ceph/pull/66565>`_, Adam Kupczyk)
* Fast failover (`pr#67150 <https://github.com/ceph/ceph/pull/67150>`_, leonidc, Leonid Chernin)
* Fix multifs auth caps check (`pr#65358 <https://github.com/ceph/ceph/pull/65358>`_, Kotresh HR)
* Form retains old data when switching from edit to create (`pr#65654 <https://github.com/ceph/ceph/pull/65654>`_, pujashahu)
* Generalize error handling for angular forms (`pr#66904 <https://github.com/ceph/ceph/pull/66904>`_, Afreen Misbah)
* github: pin GH Actions to SHA-1 commit (`pr#65761 <https://github.com/ceph/ceph/pull/65761>`_, Ernesto Puerta)
* install-deps.sh: install proper compiler version on Debian/Ubuntu (`pr#66015 <https://github.com/ceph/ceph/pull/66015>`_, Dan Mick)
* install-deps: Replace apt-mirror (`pr#66672 <https://github.com/ceph/ceph/pull/66672>`_, David Galloway)
* libcephfs: New feature - add ceph_setlk and ceph_getlk functions (`pr#65258 <https://github.com/ceph/ceph/pull/65258>`_, Giorgos Kappes)
* librbd: fix ExclusiveLock::accept_request() when !is_state_locked() (`pr#66628 <https://github.com/ceph/ceph/pull/66628>`_, Ilya Dryomov)
* librbd: introduce RBD_LOCK_MODE_EXCLUSIVE_TRANSIENT (`pr#67279 <https://github.com/ceph/ceph/pull/67279>`_, Ilya Dryomov)
* mds/FSMap: fix join_fscid being incorrectly reset for active MDS during filesystem removal (`pr#65777 <https://github.com/ceph/ceph/pull/65777>`_, ethanwu)
* mds/MDSDaemon: unlock `mds_lock` while shutting down Beacon and others (`pr#64885 <https://github.com/ceph/ceph/pull/64885>`_, Max Kellermann)
* mds: dump export_ephemeral_random_pin as double (`pr#65163 <https://github.com/ceph/ceph/pull/65163>`_, Enrico Bocchi)
* mds: fix rank 0 marked damaged if stopping fails after Elid flush (`pr#65778 <https://github.com/ceph/ceph/pull/65778>`_, ethanwu)
* mds: Fix readdir when osd is full (`pr#65346 <https://github.com/ceph/ceph/pull/65346>`_, Kotresh HR)
* mds: fix snapdiff result fragmentation (`pr#65362 <https://github.com/ceph/ceph/pull/65362>`_, Igor Fedotov, Md Mahamudur Rahaman Sajib)
* mds: include auth credential in session dump (`pr#65255 <https://github.com/ceph/ceph/pull/65255>`_, Patrick Donnelly)
* mds: Return ceph.dir.subvolume vxattr (`pr#65779 <https://github.com/ceph/ceph/pull/65779>`_, Edwin Rodriguez)
* mds: skip charmap handler check for MDS requests (`pr#64953 <https://github.com/ceph/ceph/pull/64953>`_, Patrick Donnelly)
* mds: wrong snap check for directory with parent snaps (`pr#65259 <https://github.com/ceph/ceph/pull/65259>`_, Patrick Donnelly)
* mgr/alerts: enforce ssl context to SMTP_SSL (`pr#66140 <https://github.com/ceph/ceph/pull/66140>`_, Nizamudeen A)
* mgr/cephadm: Add some new fields to the cephadm NVMEoF spec file (`pr#66987 <https://github.com/ceph/ceph/pull/66987>`_, Gil Bregman)
* mgr/cephadm: bump monitoring stack versions (`pr#65895 <https://github.com/ceph/ceph/pull/65895>`_, Nizamudeen A)
* mgr/cephadm: Change the default of max hosts per namespace in NVMEoF to 16 (`pr#66819 <https://github.com/ceph/ceph/pull/66819>`_, Gil Bregman)
* mgr/cephadm: don't mark nvmeof daemons without pool and group in name as stray (`pr#65594 <https://github.com/ceph/ceph/pull/65594>`_, Adam King)
* mgr/cephadm: update grafana conf for disconnected environment (`pr#66209 <https://github.com/ceph/ceph/pull/66209>`_, Nizamudeen A)
* mgr/cephadm: Use a persistent volume to store Loki DB (`pr#66023 <https://github.com/ceph/ceph/pull/66023>`_, Aashish Sharma)
* mgr/DaemonServer: fixed mistype for mgr_osd_messages (`pr#63345 <https://github.com/ceph/ceph/pull/63345>`_, Konstantin Shalygin)
* mgr/DaemonState: Minimise time we hold the DaemonStateIndex lock (`pr#65464 <https://github.com/ceph/ceph/pull/65464>`_, Brad Hubbard)
* mgr/dasboard : Carbonize pools form (`pr#66789 <https://github.com/ceph/ceph/pull/66789>`_, Abhishek Desai, Ankit Kumar)
* mgr/dashboard :  Fixed labels issue (`pr#66603 <https://github.com/ceph/ceph/pull/66603>`_, Abhishek Desai)
* mgr/dashboard : Carbonize -> Report an issue modal (`pr#66048 <https://github.com/ceph/ceph/pull/66048>`_, Abhishek Desai)
* mgr/dashboard : fix - about model tooltip issue (`pr#66276 <https://github.com/ceph/ceph/pull/66276>`_, Devika Babrekar)
* mgr/dashboard : fix - CephFS Authorize Modal Update issue (`pr#66419 <https://github.com/ceph/ceph/pull/66419>`_, Devika Babrekar)
* mgr/dashboard : fix css for carbon input fields (`pr#65490 <https://github.com/ceph/ceph/pull/65490>`_, Abhishek Desai)
* mgr/dashboard : Fix secure-monitoring-stack creds issue (`pr#65943 <https://github.com/ceph/ceph/pull/65943>`_, Abhishek Desai)
* mgr/dashboard : Fixed mirrored image usage info bar (`pr#65491 <https://github.com/ceph/ceph/pull/65491>`_, Abhishek Desai)
* mgr/dashboard : Fixed usage bar for secondary site in rbd mirroing (`pr#65927 <https://github.com/ceph/ceph/pull/65927>`_, Abhishek Desai)
* mgr/dashboard : Fixed warning icon colour issue with carbon colour (`pr#66271 <https://github.com/ceph/ceph/pull/66271>`_, Abhishek Desai)
* mgr/dashboard : Hide suppressed  alert on landing page (`pr#65737 <https://github.com/ceph/ceph/pull/65737>`_, Abhishek Desai)
* mgr/dashboard : Remove subalerts details for multiple subalerts (`pr#66295 <https://github.com/ceph/ceph/pull/66295>`_, Abhishek Desai)
* mgr/dashboard : Skip calls until secure_monitoring_stack is enabled (`pr#65673 <https://github.com/ceph/ceph/pull/65673>`_, Abhishek Desai)
* mgr/dashboard: --no-group-append default value to False, aligned with old cli" (`pr#65678 <https://github.com/ceph/ceph/pull/65678>`_, Tomer Haskalovitch)
* mgr/dashboard: Add Archive zone configuration to the Dashboard (`pr#67131 <https://github.com/ceph/ceph/pull/67131>`_, Aashish Sharma)
* mgr/dashboard: add customizations to table-actions (`pr#65956 <https://github.com/ceph/ceph/pull/65956>`_, Naman Munet)
* mgr/dashboard: Add full page tearsheet component (`pr#66892 <https://github.com/ceph/ceph/pull/66892>`_, Afreen Misbah)
* mgr/dashboard: Add generic wizard component (`pr#66893 <https://github.com/ceph/ceph/pull/66893>`_, Afreen Misbah)
* mgr/dashboard: add get_subsystem nvme command (`pr#66941 <https://github.com/ceph/ceph/pull/66941>`_, Tomer Haskalovitch)
* mgr/dashboard: add indentation to the json output of nvmeof cli commands (`pr#66940 <https://github.com/ceph/ceph/pull/66940>`_, Tomer Haskalovitch)
* mgr/dashboard: add multiple ceph users deletion (`pr#65658 <https://github.com/ceph/ceph/pull/65658>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: add nsid param to ns add command (`pr#65677 <https://github.com/ceph/ceph/pull/65677>`_, Tomer Haskalovitch)
* mgr/dashboard: add nsid param to ns list command (`pr#65749 <https://github.com/ceph/ceph/pull/65749>`_, Tomer Haskalovitch)
* mgr/dashboard: Add overview page and change 'Dashboard' to 'Overview' (`pr#67118 <https://github.com/ceph/ceph/pull/67118>`_, Afreen Misbah)
* mgr/dashboard: Add productive card component (`pr#67147 <https://github.com/ceph/ceph/pull/67147>`_, Afreen Misbah)
* mgr/dashboard: add text-label-list component (`pr#66312 <https://github.com/ceph/ceph/pull/66312>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: Adding QAT Compression dropdown on RGW Service form (`pr#66642 <https://github.com/ceph/ceph/pull/66642>`_, Devika Babrekar)
* mgr/dashboard: allow deletion of non-default zone and zonegroup (`pr#66211 <https://github.com/ceph/ceph/pull/66211>`_, Aashish Sharma)
* mgr/dashboard: Allow FQDN in Connect Cluster form -> Cluster API URL (`pr#65622 <https://github.com/ceph/ceph/pull/65622>`_, Aashish Sharma)
* mgr/dashboard: Blank entry for Storage Capacity in dashboard under Cluster > Expand Cluster > Review (`pr#65705 <https://github.com/ceph/ceph/pull/65705>`_, Naman Munet)
* mgr/dashboard: bump validator package to address vulnerability (`pr#66227 <https://github.com/ceph/ceph/pull/66227>`_, Naman Munet)
* mgr/dashboard: Carbonize - Multisite Zone (`pr#67117 <https://github.com/ceph/ceph/pull/67117>`_, Dnyaneshwari Talwekar)
* mgr/dashboard: Carbonize Administration module > Create Realm/Zone group/zone (`pr#66986 <https://github.com/ceph/ceph/pull/66986>`_, Dnyaneshwari Talwekar)
* mgr/dashboard: Carbonize multisite sync policy forms (`pr#66302 <https://github.com/ceph/ceph/pull/66302>`_, Naman Munet)
* mgr/dashboard: carbonize service form (`pr#66978 <https://github.com/ceph/ceph/pull/66978>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: Carbonize the Change Password Form (`pr#66401 <https://github.com/ceph/ceph/pull/66401>`_, Afreen Misbah)
* mgr/dashboard: carbonize-delete-zone-modal (`pr#67100 <https://github.com/ceph/ceph/pull/67100>`_, Sagar Gopale)
* mgr/dashboard: carbonize-delete-zonegroup-modal (`pr#67014 <https://github.com/ceph/ceph/pull/67014>`_, Sagar Gopale)
* mgr/dashboard: carbonized-multisite-export-realm-token-modal (`pr#66649 <https://github.com/ceph/ceph/pull/66649>`_, Sagar Gopale)
* mgr/dashboard: change the default max namespace from 4096 to None in subsystem add command (`pr#65951 <https://github.com/ceph/ceph/pull/65951>`_, Tomer Haskalovitch)
* mgr/dashboard: Edit user via UI throwing multiple server errors (`pr#66081 <https://github.com/ceph/ceph/pull/66081>`_, Naman Munet)
* mgr/dashboard: empty-data-message (`pr#66902 <https://github.com/ceph/ceph/pull/66902>`_, Sagar Gopale)
* mgr/dashboard: fetch all namespaces in a gateway group (`pr#67140 <https://github.com/ceph/ceph/pull/67140>`_, Afreen Misbah)
* mgr/dashboard: fix command alias help message (`pr#65750 <https://github.com/ceph/ceph/pull/65750>`_, Tomer Haskalovitch)
* mgr/dashboard: fix dashboard freeze on missing smb permissions (`pr#65873 <https://github.com/ceph/ceph/pull/65873>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fix data mismatch in Advance section in Tiering (`pr#65672 <https://github.com/ceph/ceph/pull/65672>`_, Dnyaneshwari Talwekar)
* mgr/dashboard: Fix display of IP address in host page (`pr#67146 <https://github.com/ceph/ceph/pull/67146>`_, Afreen Misbah)
* mgr/dashboard: fix icon alignment in navigation header (`pr#66091 <https://github.com/ceph/ceph/pull/66091>`_, Naman Munet)
* mgr/dashboard: fix misaligned text links on login page (`pr#66052 <https://github.com/ceph/ceph/pull/66052>`_, prik73, Afreen Misbah)
* mgr/dashboard: fix missing schedule interval in rbd API (`pr#65560 <https://github.com/ceph/ceph/pull/65560>`_, Nizamudeen A)
* mgr/dashboard: fix multi-cluster route reload logic (`pr#66504 <https://github.com/ceph/ceph/pull/66504>`_, Aashish Sharma)
* mgr/dashboard: fix multisite wizard realm configuration mode (`pr#66017 <https://github.com/ceph/ceph/pull/66017>`_, Aashish Sharma)
* mgr/dashboard: fix None force param handling in ns add_host so it won't raise exceptions (`pr#65679 <https://github.com/ceph/ceph/pull/65679>`_, Tomer Haskalovitch)
* mgr/dashboard: fix ns add and resize commands help (`pr#66939 <https://github.com/ceph/ceph/pull/66939>`_, Tomer Haskalovitch)
* mgr/dashboard: fix oauth2-service creation UI error (`pr#66139 <https://github.com/ceph/ceph/pull/66139>`_, Nizamudeen A)
* mgr/dashboard: fix prometheus API error when not configured (`pr#65856 <https://github.com/ceph/ceph/pull/65856>`_, Nizamudeen A)
* mgr/dashboard: fix rbd form mirroring toggle (`pr#65874 <https://github.com/ceph/ceph/pull/65874>`_, Nizamudeen A)
* mgr/dashboard: fix RBD mirror schedule inheritance in pool and image APIs (`pr#67107 <https://github.com/ceph/ceph/pull/67107>`_, Imran Imtiaz)
* mgr/dashboard: fix smb button and table column (`pr#65657 <https://github.com/ceph/ceph/pull/65657>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: Fix table width expansion on manager module dropdown selection #74089 (`pr#66647 <https://github.com/ceph/ceph/pull/66647>`_, Sagar Gopale)
* mgr/dashboard: fix the separation between CLI and API only commands (`pr#65781 <https://github.com/ceph/ceph/pull/65781>`_, Tomer Haskalovitch)
* mgr/dashboard: Fix timestamps in APIs (`pr#66029 <https://github.com/ceph/ceph/pull/66029>`_, Afreen Misbah)
* mgr/dashboard: fix total capacity value in dashboard (`pr#65647 <https://github.com/ceph/ceph/pull/65647>`_, Nizamudeen A)
* mgr/dashboard: fix typo in error when gw does not exist (`pr#66956 <https://github.com/ceph/ceph/pull/66956>`_, Tomer Haskalovitch)
* mgr/dashboard: fix zone update API forcing STANDARD storage class (`pr#65619 <https://github.com/ceph/ceph/pull/65619>`_, Aashish Sharma)
* mgr/dashboard: fixes for quick-bootstrap script (`pr#67040 <https://github.com/ceph/ceph/pull/67040>`_, Nizamudeen A)
* mgr/dashboard: FS - Attach Command showing undefined for MountData (`pr#65675 <https://github.com/ceph/ceph/pull/65675>`_, Dnyaneshwari Talwekar)
* mgr/dashboard: Group similar alerts (`pr#65493 <https://github.com/ceph/ceph/pull/65493>`_, Abhishek Desai)
* mgr/dashboard: Handle pool creation in tiering local storage class creation (`pr#65680 <https://github.com/ceph/ceph/pull/65680>`_, Dnyaneshwari, Naman Munet)
* mgr/dashboard: Maintain sentence case consistency in side nav bar titles (`pr#66050 <https://github.com/ceph/ceph/pull/66050>`_, Aashish Sharma)
* mgr/dashboard: ns list now support not passing nqn param (`pr#65897 <https://github.com/ceph/ceph/pull/65897>`_, Tomer Haskalovitch)
* mgr/dashboard: raise exception if both size and rbd_image_size are being passed in ns add (`pr#65816 <https://github.com/ceph/ceph/pull/65816>`_, Tomer Haskalovitch)
* mgr/dashboard: rbd consistency group and snapshot APIs (`pr#66935 <https://github.com/ceph/ceph/pull/66935>`_, Imran Imtiaz)
* mgr/dashboard: Remove illegible texts from the dashboard (`pr#66306 <https://github.com/ceph/ceph/pull/66306>`_, Afreen Misbah)
* mgr/dashboard: remove not needed 'cli_version' field from gw info com… (`pr#66942 <https://github.com/ceph/ceph/pull/66942>`_, Tomer Haskalovitch)
* mgr/dashboard: Remove the time dropdown from grafana iframe (`pr#65853 <https://github.com/ceph/ceph/pull/65853>`_, Abhishek Desai)
* mgr/dashboard: removes nx folder (`pr#67003 <https://github.com/ceph/ceph/pull/67003>`_, Afreen Misbah)
* mgr/dashboard: rename 'Zone Group' labels to 'Zonegroup' (`pr#66790 <https://github.com/ceph/ceph/pull/66790>`_, Sagar Gopale)
* mgr/dashboard: Rename Alerts tab to All Alerts (`pr#66532 <https://github.com/ceph/ceph/pull/66532>`_, Sagar Gopale)
* mgr/dashboard: Rename side-nav panel items (`pr#65846 <https://github.com/ceph/ceph/pull/65846>`_, Naman Munet)
* mgr/dashboard: replace bootstrap badges with carbon tags (`pr#66350 <https://github.com/ceph/ceph/pull/66350>`_, pujaoshahu)
* mgr/dashboard: replace usage or progress bar with carbon meter chart (`pr#66934 <https://github.com/ceph/ceph/pull/66934>`_, Naman Munet)
* mgr/dashboard: rgw accounts form group mode disable option is not working (`pr#66351 <https://github.com/ceph/ceph/pull/66351>`_, Naman Munet)
* mgr/dashboard: server side table rendering improvements (`pr#65828 <https://github.com/ceph/ceph/pull/65828>`_, Nizamudeen A)
* mgr/dashboard: service creation fails if service name is same as sevice type (`pr#66481 <https://github.com/ceph/ceph/pull/66481>`_, Naman Munet)
* mgr/dashboard: Set max subsystem count to 512 rather than 4096 (`pr#66284 <https://github.com/ceph/ceph/pull/66284>`_, Afreen Misbah)
* mgr/dashboard: support gw get_stats and listener info (`pr#65896 <https://github.com/ceph/ceph/pull/65896>`_, Tomer Haskalovitch)
* mgr/dashboard: Tiering form - Placement Target in Advanced Section (`pr#65653 <https://github.com/ceph/ceph/pull/65653>`_, Dnyaneshwari Talwekar)
* mgr/dashboard: update teuth_ref hash in api test (`pr#66706 <https://github.com/ceph/ceph/pull/66706>`_, Nizamudeen A)
* mgr/dashboard:[NFS] add Subvolume Groups and Subvolumes in "Edit NFS Export form" (`pr#65650 <https://github.com/ceph/ceph/pull/65650>`_, Dnyaneshwari Talwekar)
* mgr/prometheus: Handle empty/invalid JSON from orch get-security-config (`pr#65906 <https://github.com/ceph/ceph/pull/65906>`_, Sunnatillo)
* mgr/telemetry: add 'ec_optimizations' flag to 'basic_pool_flags' collection (`pr#65969 <https://github.com/ceph/ceph/pull/65969>`_, Laura Flores)
* mgr/vol: handling the failed non-atomic operation (`pr#65728 <https://github.com/ceph/ceph/pull/65728>`_, Neeraj Pratap Singh)
* mgr/vol: keep and show clone source info (`pr#64650 <https://github.com/ceph/ceph/pull/64650>`_, Rishabh Dave)
* mgr/volumes: Keep mon caps if auth key has remaining mds/osd caps (`pr#65262 <https://github.com/ceph/ceph/pull/65262>`_, Enrico Bocchi)
* mgr/volumes: remove unnecessary log error lines from earmark handling (`pr#66991 <https://github.com/ceph/ceph/pull/66991>`_, Avan Thakkar)
* mgr: avoid explicit dropping of ref (`pr#65005 <https://github.com/ceph/ceph/pull/65005>`_, Milind Changire)
* mgr:python: avoid pyo3 errors by running certain cryptographic functions in a child process (`pr#66794 <https://github.com/ceph/ceph/pull/66794>`_, Nizamudeen A, John Mulligan, Paulo E. Castro)
* mon/FSCommands: avoid unreachable code triggering compiler warning (`pr#65261 <https://github.com/ceph/ceph/pull/65261>`_, Patrick Donnelly)
* mon/MgrMonitor: add a space before "is already disabled" (`pr#64687 <https://github.com/ceph/ceph/pull/64687>`_, Zehua Qi)
* mon/OSDMonitor.cc: optionally display availability status in json (`pr#65794 <https://github.com/ceph/ceph/pull/65794>`_, Shraddha Agrawal)
* mon: Add command "nvme-gw listeners" (`pr#66584 <https://github.com/ceph/ceph/pull/66584>`_, Vallari Agrawal)
* mon: ceph pg repeer should propose a correctly sized pg temp (`pr#66324 <https://github.com/ceph/ceph/pull/66324>`_, Alex Ainscow)
* mon: Deny EC optimizations (fast EC) for non-4k-aligned chunk-sizes (`pr#67319 <https://github.com/ceph/ceph/pull/67319>`_, Alex Ainscow)
* monc: synchronize tick() of MonClient with shutdown() (`pr#66916 <https://github.com/ceph/ceph/pull/66916>`_, Radoslaw Zarzynski)
* monitoring: fix "In" OSDs in Cluster-Advanced grafana panel. Also change units from decbytes to bytes wherever used in the panel (`pr#65670 <https://github.com/ceph/ceph/pull/65670>`_, Aashish Sharma)
* monitoring: fix "Total gateway" and "Ceph Health NVMeoF WARNING" grafana graphs (`pr#66225 <https://github.com/ceph/ceph/pull/66225>`_, Vallari Agrawal)
* monitoring: fix CephPgImbalance alert rule expression (`pr#66828 <https://github.com/ceph/ceph/pull/66828>`_, Aashish Sharma)
* monitoring: Fix Filesystem grafana dashboard units (`pr#66018 <https://github.com/ceph/ceph/pull/66018>`_, Ankush Behl)
* monitoring: fix MTU Mismatch alert rule and expr (`pr#65708 <https://github.com/ceph/ceph/pull/65708>`_, Aashish Sharma)
* monitoring: fix rgw_servers filtering in rgw sync overview grafana (`pr#66989 <https://github.com/ceph/ceph/pull/66989>`_, Aashish Sharma)
* monitoring: Fixes for smb overview (`pr#66019 <https://github.com/ceph/ceph/pull/66019>`_, Ankush Behl)
* monitoring: make cluster matcher backward compatible for pre-reef metrics (`pr#66984 <https://github.com/ceph/ceph/pull/66984>`_, Aashish Sharma)
* monitoring: update NVMeoFTooManyNamespaces to 4096 ns (`pr#67039 <https://github.com/ceph/ceph/pull/67039>`_, Vallari Agrawal)
* monitoring: upgrade grafana version to 12.3.1 (`pr#66963 <https://github.com/ceph/ceph/pull/66963>`_, Aashish Sharma)
* nvmeof: refactor beacon timer for exact frequency timing with drift correction (`pr#66536 <https://github.com/ceph/ceph/pull/66536>`_, Alexander Indenbaum)
* Objecter: respect higher epoch subscription in tick (`pr#66972 <https://github.com/ceph/ceph/pull/66972>`_, Nitzan Mordechai)
* os/bluestore: cumulative patch to fix extent map resharding and around (`pr#65964 <https://github.com/ceph/ceph/pull/65964>`_, Igor Fedotov, Adam Kupczyk, Jaya Prakash)
* os/bluestore: fix vselector update after enveloped WAL recovery (`pr#67333 <https://github.com/ceph/ceph/pull/67333>`_, Igor Fedotov, Adam Kupczyk)
* os/bluestore: introduce device type specific allocation policy (`pr#66839 <https://github.com/ceph/ceph/pull/66839>`_, Igor Fedotov)
* osd/ECUtil: Fix erase_after_ro_offset length calculation and add tests (`pr#66825 <https://github.com/ceph/ceph/pull/66825>`_, Alex Ainscow)
* osd/PeeringState: re-evaluate full OSDs while waiting for recovery re… (`pr#65701 <https://github.com/ceph/ceph/pull/65701>`_, Nitzan Mordechai)
* osd/scrub: do not reduce min chunk on preemption (`pr#66214 <https://github.com/ceph/ceph/pull/66214>`_, Ronen Friedman)
* osd/scrub: fix blocked scrub accounting (`pr#66220 <https://github.com/ceph/ceph/pull/66220>`_, Ronen Friedman)
* osd/scrub: new/modified perf counters for scrub preemption (`pr#66234 <https://github.com/ceph/ceph/pull/66234>`_, Ronen Friedman)
* osd: Do not remove objects with divergent logs if only partial writes (`pr#66725 <https://github.com/ceph/ceph/pull/66725>`_, Alex Ainscow)
* osd: Fix fast EC truncate to whole stripe (`pr#66543 <https://github.com/ceph/ceph/pull/66543>`_, Alex Ainscow)
* osd: Fix for num_bytes mismatch occurring from snapshot workloads with partial writes in fast_ec (`pr#67137 <https://github.com/ceph/ceph/pull/67137>`_, Jon Bailey)
* osd: Fix memory leak of ECDummyOp (`pr#66977 <https://github.com/ceph/ceph/pull/66977>`_, Alex Ainscow)
* osd: Fix stats mismatch cluster error seen during scrubbing occasionally (`pr#65793 <https://github.com/ceph/ceph/pull/65793>`_, Jon Bailey)
* osd: Relax missing entry assert for partial writes (`pr#65860 <https://github.com/ceph/ceph/pull/65860>`_, Alex Ainscow)
* osd: stop scrub_purged_snaps() from ignoring osd_beacon_report_interval (`pr#65478 <https://github.com/ceph/ceph/pull/65478>`_, Radoslaw Zarzynski)
* pickup object corpus 20.2.0 380 gdbcbbd3f281 (`pr#66592 <https://github.com/ceph/ceph/pull/66592>`_, Nitzan Mordechai)
* prometheus: Add Cephadm orch ps output metric to prometheus (`pr#66760 <https://github.com/ceph/ceph/pull/66760>`_, Ankush Behl)
* pybind/mgr/dashboard: dashboard/requirements-lint.txt: re-pin rsscheck (`pr#66877 <https://github.com/ceph/ceph/pull/66877>`_, Ronen Friedman)
* pybind/mgr/pg_autoscaler: Introduce dynamic threshold to improve scal… (`pr#66871 <https://github.com/ceph/ceph/pull/66871>`_, Prashant D)
* pybind/mgr: pin cheroot version in requirements-required.txt (`pr#65635 <https://github.com/ceph/ceph/pull/65635>`_, Adam King)
* pybind/rados: Add list_lockers() and break_lock() to Rados Python interface (`pr#65098 <https://github.com/ceph/ceph/pull/65098>`_, Gil Bregman)
* qa/multisite: switch to boto3 (`pr#67318 <https://github.com/ceph/ceph/pull/67318>`_, Shilpa Jagannath, Adam C. Emerson)
* qa/rgw: bucket notifications use pynose (`pr#67449 <https://github.com/ceph/ceph/pull/67449>`_, Casey Bodley, Adam C. Emerson)
* qa/standalone/availability.sh: retry after feature is turned on (`pr#67226 <https://github.com/ceph/ceph/pull/67226>`_, Shraddha Agrawal)
* qa/suites/nvmeof: add upgrade sub-suite (`pr#65583 <https://github.com/ceph/ceph/pull/65583>`_, Vallari Agrawal)
* qa/suites/rados/thrash-old-clients: Add OSD warnings to ignore list (`pr#65369 <https://github.com/ceph/ceph/pull/65369>`_, Naveen Naidu)
* qa/suites/rbd/valgrind: don't hardcode os_type in memcheck.yaml (`pr#66196 <https://github.com/ceph/ceph/pull/66196>`_, Ilya Dryomov)
* qa/suites/upgrade: add "Replacing daemon mds" to ignorelist (`issue#50279 <http://tracker.ceph.com/issues/50279>`_, `issue#71615 <http://tracker.ceph.com/issues/71615>`_, `pr#64888 <https://github.com/ceph/ceph/pull/64888>`_, Venky Shankar)
* qa/suites: wait longer before stopping OSDs with valgrind (`pr#63716 <https://github.com/ceph/ceph/pull/63716>`_, Nitzan Mordechai)
* qa/tasks/ceph_manager: population must be a sequence (`pr#64746 <https://github.com/ceph/ceph/pull/64746>`_, Kyr Shatskyy)
* qa/tasks/qemu: rocky 10 enablement (`pr#67283 <https://github.com/ceph/ceph/pull/67283>`_, Ilya Dryomov)
* qa/tasks/rbd_mirror_thrash: don't use random.randrange() on floats (`pr#67163 <https://github.com/ceph/ceph/pull/67163>`_, Ilya Dryomov)
* qa/tasks/workunit: fix no module named 'pipes' (`pr#66250 <https://github.com/ceph/ceph/pull/66250>`_, Kyr Shatskyy)
* qa/tests: added inital draft for tentacle-p2p (`pr#67765 <https://github.com/ceph/ceph/pull/67765>`_, Patrick Donnelly, Yuri Weinstein)
* qa/tests: added messages to the whitelist (`pr#65645 <https://github.com/ceph/ceph/pull/65645>`_, Laura Flores, Yuri Weinstein)
* qa/tests: wait for module to be available for connection (`pr#67196 <https://github.com/ceph/ceph/pull/67196>`_, Nizamudeen A)
* qa/valgrind.supp: make gcm_cipher_internal suppression more resilient (`pr#67281 <https://github.com/ceph/ceph/pull/67281>`_, Ilya Dryomov)
* qa/workunits/nvmeof/basic_tests: use nvme-cli 2.13 (`pr#67285 <https://github.com/ceph/ceph/pull/67285>`_, Vallari Agrawal)
* qa/workunits/rados: remove cache tier test (`pr#65540 <https://github.com/ceph/ceph/pull/65540>`_, Nitzan Mordechai)
* qa/workunits/rbd: adapt rbd_mirror.sh for trial nodes (`pr#67152 <https://github.com/ceph/ceph/pull/67152>`_, Ilya Dryomov)
* qa/workunits/rbd: reduce randomized sleeps in live import tests (`pr#67154 <https://github.com/ceph/ceph/pull/67154>`_, Ilya Dryomov)
* qa/workunits/rbd: use the same qemu-iotests version throughout (`pr#67282 <https://github.com/ceph/ceph/pull/67282>`_, Ilya Dryomov)
* qa/workunits/rgw: drop netstat usage (`pr#67184 <https://github.com/ceph/ceph/pull/67184>`_, Kyr Shatskyy)
* qa/workunits: add Rocky Linux support to librados tests (`pr#67091 <https://github.com/ceph/ceph/pull/67091>`_, Nitzan Mordechai)
* qa: Disable OSD benchmark from running for tests (`pr#67068 <https://github.com/ceph/ceph/pull/67068>`_, Sridhar Seshasayee)
* qa: don't assume that /dev/sda or /dev/vda is present in unmap.t (`pr#67077 <https://github.com/ceph/ceph/pull/67077>`_, Ilya Dryomov)
* qa: Fix test_with_health_warn_with_2_active_MDSs (`pr#65260 <https://github.com/ceph/ceph/pull/65260>`_, Kotresh HR)
* qa: ignore cluster warning (evicting unresponsive ...) with tasks/mgr-osd-full (`issue#73278 <http://tracker.ceph.com/issues/73278>`_, `pr#66125 <https://github.com/ceph/ceph/pull/66125>`_, Venky Shankar)
* qa: Improve scalability test (`pr#66224 <https://github.com/ceph/ceph/pull/66224>`_, Vallari Agrawal)
* qa: krbd_blkroset.t: eliminate a race in the open_count test (`pr#67075 <https://github.com/ceph/ceph/pull/67075>`_, Ilya Dryomov)
* qa: Run RADOS suites with ec optimizations on and off (`pr#65471 <https://github.com/ceph/ceph/pull/65471>`_, Jamie Pryde)
* qa: suppress OpenSSL valgrind leaks (`pr#65660 <https://github.com/ceph/ceph/pull/65660>`_, Laura Flores)
* rbd-mirror: add cluster fsid to remote meta cache key (`pr#66297 <https://github.com/ceph/ceph/pull/66297>`_, Mykola Golub)
* rbd-mirror: allow incomplete demote snapshot to sync after rbd-mirror daemon restart (`pr#66164 <https://github.com/ceph/ceph/pull/66164>`_, VinayBhaskar-V)
* Relax scrub of shard sizes for upgraded EC pools (`pr#66021 <https://github.com/ceph/ceph/pull/66021>`_, Alex Ainscow)
* Revert "Merge pull request #66958 from Hezko/wip-74413-tentacle" (`pr#67750 <https://github.com/ceph/ceph/pull/67750>`_, Patrick Donnelly)
* Revert "PrimeryLogPG: don't accept ops with mixed balance_reads and rwordered flags" (`pr#66611 <https://github.com/ceph/ceph/pull/66611>`_, Radoslaw Zarzynski)
* RGW | fix conditional Delete, MultiDelete and Put (`pr#65949 <https://github.com/ceph/ceph/pull/65949>`_, Ali Masarwa)
* RGW | fix conditional MultiWrite (`pr#67425 <https://github.com/ceph/ceph/pull/67425>`_, Ali Masarwa)
* rgw/account: bucket acls are not completely migrated once the user is migrated to an account (`pr#65666 <https://github.com/ceph/ceph/pull/65666>`_, kchheda3)
* rgw/admin: Add max-entries and marker to bucket list (`pr#65485 <https://github.com/ceph/ceph/pull/65485>`_, Tobias Urdin)
* rgw/lc: LCOpAction_CurrentExpiration checks mtime for delete markers (`pr#65965 <https://github.com/ceph/ceph/pull/65965>`_, Casey Bodley)
* rgw/tentacle: clean up .rgw_op.cc.swn file (`pr#66161 <https://github.com/ceph/ceph/pull/66161>`_, Soumya Koduri)
* rgw: add metric when send message with kafka and ampq (`pr#65904 <https://github.com/ceph/ceph/pull/65904>`_, Hoai-Thu Vuong)
* rgw: fix 'bucket rm --bypass-gc' for copied objects (`pr#66004 <https://github.com/ceph/ceph/pull/66004>`_, Casey Bodley)
* rgw: fix `radosgw-admin object unlink ...` (`pr#66151 <https://github.com/ceph/ceph/pull/66151>`_, J. Eric Ivancich)
* RGW: multi object delete op; skip olh update for all deletes but the last one (`pr#65488 <https://github.com/ceph/ceph/pull/65488>`_, Oguzhan Ozmen)
* rgw: update keystone repo stable branch to 2024.2 (`pr#66241 <https://github.com/ceph/ceph/pull/66241>`_, Kyr Shatskyy)
* rpm: default to gcc-toolset-13, not just for crimson (`pr#65752 <https://github.com/ceph/ceph/pull/65752>`_, John Mulligan, Casey Bodley)
* scripts/build/ceph.spec.in: fix rhel version checks (`pr#66865 <https://github.com/ceph/ceph/pull/66865>`_, Ronen Friedman)
* src/ceph_osd, osd: Implement running benchmark during OSD creation - Phase 1 (`pr#65522 <https://github.com/ceph/ceph/pull/65522>`_, Sridhar Seshasayee)
* src: Move the decision to build the ISA plugin to the top level make file (`pr#67894 <https://github.com/ceph/ceph/pull/67894>`_, Alex Ainscow)
* sync build-with-container patches from main (`pr#65843 <https://github.com/ceph/ceph/pull/65843>`_, John Mulligan, Dan Mick)
* systemd services: fix installing ceph-volume@ (`pr#66861 <https://github.com/ceph/ceph/pull/66861>`_, Thomas Lamprecht)
* tasks/cbt_performance: Tolerate exceptions during performance data up… (`pr#66102 <https://github.com/ceph/ceph/pull/66102>`_, Nitzan Mordechai)
* test/ceph_assert.cc: Disable core files (`pr#66334 <https://github.com/ceph/ceph/pull/66334>`_, Bob Ham)
* test/neorados: Catch timeouts in Poll test (`pr#65605 <https://github.com/ceph/ceph/pull/65605>`_, Adam C. Emerson)
* test: disable known flaky tests in run-rbd-unit-tests (`pr#67559 <https://github.com/ceph/ceph/pull/67559>`_, Ilya Dryomov)
* tools: handle get-attr as read-only ops in ceph_objectstore_tool (`pr#66537 <https://github.com/ceph/ceph/pull/66537>`_, Jaya Prakash)

v20.2.0 Tentacle
================

Release Date
------------

November 18, 2025

Highlights
----------

*See the sections below for more details on these items.*

CephFS

* Directories may now be configured with case-insensitive or normalized
  directory entry names.
* Modifying the FS setting variable ``max_mds`` when a cluster is unhealthy
  now requires users to pass the confirmation flag (``--yes-i-really-mean-it``).
* ``EOPNOTSUPP`` (Operation not supported) is now returned by the CephFS FUSE
  client for ``fallocate`` for the default case (i.e. ``mode == 0``).

Dashboard

* Support has been added for NVMe/TCP gateway groups and multiple
  namespaces, multi-cluster management, OAuth 2.0 integration, and enhanced
  RGW/SMB features including multi-site automation, tiering, policies,
  lifecycles, notifications, and granular replication.

Integrated SMB support

* Ceph clusters now offer an SMB Manager module that works like the existing
  NFS subsystem. The new SMB support allows the Ceph cluster to automatically
  create Samba-backed SMB file shares connected to CephFS. The ``smb`` module
  can configure both basic Active Directory domain or standalone user
  authentication. The Ceph cluster can host one or more virtual SMB clusters
  which can be truly clustered using Samba's CTDB technology. The ``smb``
  module requires a cephadm-enabled Ceph cluster and deploys container images
  provided by the ``samba-container`` project. The Ceph dashboard can be used
  to configure SMB clusters and shares. A new ``cephfs-proxy`` daemon is
  automatically deployed to improve scalability and memory usage when connecting
  Samba to CephFS.

MGR

* Users now have the ability to force-disable always-on modules.
* The ``restful`` and ``zabbix`` modules (deprecated since 2020) have been
  officially removed.

RADOS

* FastEC: Long-anticipated performance and space amplification
  optimizations are added for erasure-coded pools.
* BlueStore: Improved compression and a new, faster WAL (write-ahead-log).
* Data Availability Score: Users can now track a data availability score
  for each pool in their cluster.
* OMAP: All components have been switched to the faster OMAP iteration
  interface, which improves RGW bucket listing and scrub operations.

Crimson

* SeaStore Tech Preview: SeaStore object store is now deployable
  alongside Crimson-OSD, mainly for early testing and experimentation.
  Community feedback is encouraged to help with future improvements.

RBD

* New live migration features: RBD images can now be instantly imported
  from another Ceph cluster (native format) or from a wide variety of
  external sources/formats.
* There is now support for RBD namespace remapping while mirroring between
  Ceph clusters.
* Several commands related to group and group snap info were added or
  improved, and ``rbd device map`` command now defaults to ``msgr2``.

RGW

* Added support for S3 ``GetObjectAttributes``.
* For compatibility with AWS S3, ``LastModified`` timestamps are now truncated
  to the second. Note that during upgrade, users may observe these timestamps
  moving backwards as a result.
* Bucket resharding now does most of its processing before it starts to block
  write operations. This should significantly reduce the client-visible impact
  of resharding on large buckets.
* RGW: The User Account feature introduced in Squid provides first-class support for
  IAM APIs and policy. Our preliminary STS support was based on tenants, and
  exposed some IAM APIs to admins only. This tenant-level IAM functionality is now
  deprecated in favor of accounts. While we'll continue to support the tenant feature
  itself for namespace isolation, the following features will be removed no sooner
  than the V release:

    * Tenant-level IAM APIs including CreateRole, PutRolePolicy and PutUserPolicy,
    * Use of tenant names instead of accounts in IAM policy documents,
    * Interpretation of IAM policy without cross-account policy evaluation,
    * S3 API support for cross-tenant names such as `Bucket='tenant:bucketname'`
    * STS Lite and `sts:GetSessionToken`.

Cephadm
-------

* A new cephadm-managed ``mgmt-gateway`` service provides a single, TLS-terminated
  entry point for Ceph management endpoints such as the Dashboard and the monitoring
  stack. The gateway is implemented as an nginx-based reverse proxy that fronts Prometheus,
  Grafana, and Alertmanager, so users no longer need to connect to those daemons directly or
  know which hosts they run on. When combined with the new ``oauth2-proxy`` service, which
  integrates with external identity providers using the OpenID Connect (OIDC) / OAuth 2.0
  protocols, the gateway can enforce centralized authentication and single sign-on (SSO) for
  both the Ceph Dashboard and the rest of the monitoring stack.
* High availability for the Ceph Dashboard and the Prometheus-based monitoring stack is now
  provided via the cephadm-managed ``mgmt-gateway``. nginx high-availability mechanisms allow
  the mgmt-gateway to detect healthy instances of the Dashboard, Prometheus, Grafana, and Alertmanager,
  route traffic accordingly, and handle manager failover transparently. When deployed with a virtual
  IP and multiple ``mgmt-gateway`` instances, this architecture keeps management access available
  even during daemon or host failures.
* A new ``certmgr`` cephadm subsystem centralizes certificate lifecycle management for cephadm-managed
  services. certmgr acts as a cluster-internal root CA for cephadm-signed certificates, it can also
  consume user-provided certificates, and tracks how each certificate was provisioned. It standardizes
  HTTPS configuration for services such as RGW and the mgmt-gateway, automates renewal and rotation of
  cephadm-signed certificates, and raises health warnings when certificates are invalid, expiring or misconfigured.
  With certmgr, cephadm-signed certificates are available across all cephadm-managed services, providing
  secure defaults out of the box.
    
CephFS
------

* Directories may now be configured with case-insensitive or
  normalized directory entry names. This is an inheritable configuration,
  making it apply to an entire directory tree.

  For more information, see :ref:`charmap`.

* It is now possible to pause the threads that asynchronously purge
  deleted subvolumes by using the config option
  ``mgr/volumes/pause_purging``.

* It is now possible to pause the threads that asynchronously clone
  subvolume snapshots by using the config option
  ``mgr/volumes/pause_cloning``.

* Modifying the setting ``max_mds`` when a cluster is
  unhealthy now requires users to pass the confirmation flag
  (``--yes-i-really-mean-it``). This has been added as a precaution to inform
  users that modifying ``max_mds`` may not help with troubleshooting or recovery
  efforts. Instead, it might further destabilize the cluster.

* ``EOPNOTSUPP`` (Operation not supported) is now returned by the CephFS
  FUSE client for ``fallocate`` in the default case (i.e., ``mode == 0``) since
  CephFS does not support disk space reservation. The only flags supported are
  ``FALLOC_FL_KEEP_SIZE`` and ``FALLOC_FL_PUNCH_HOLE``.

* The ``ceph fs subvolume snapshot getpath`` command now allows users
  to get the path of a snapshot of a subvolume. If the snapshot is not present,
  ``ENOENT`` is returned.

* The ``ceph fs volume create`` command now allows users to pass
  metadata and data pool names to be used for creating the volume. If either
  is not passed, or if either is a non-empty pool, the command will abort.

* The format of the pool namespace name for CephFS volumes has been changed
  from ``fsvolumens__<subvol-name>`` to
  ``fsvolumens__<subvol-grp-name>_<subvol-name>`` to avoid namespace collisions
  when two subvolumes located in different subvolume groups have the same name.
  Even with namespace collisions, there were no security issues, since the MDS
  auth cap is restricted to the subvolume path. Now, with this change, the
  namespaces are completely isolated.

* If the subvolume name passed to the command ``ceph fs subvolume info``
  is a clone, the output will now also contain a "source" field that tells the
  user the name of the source snapshot along with the name of the volume,
  subvolume group, and subvolume in which the source snapshot is located.
  For clones created with Tentacle or an earlier release, the value of this
  field will be ``N/A``. Regular subvolumes do not have a source subvolume and
  therefore the output for them will not contain a "source" field regardless of
  the release.

Crimson / SeaStore
------------------

The Crimson project continues to progress, with the Squid release marking the
first technical preview available for Crimson.
The Tentacle release introduces a host of improvements and new functionalities
that enhance the robustness, performance, and usability
of both Crimson-OSD and the SeaStore object store.
In this release, SeaStore can now be deployed alongside the Crimson-OSD!
Early testing and experimentation are highly encouraged and we’d greatly
appreciate any initial feedback rounds from the community to help guide future
improvements.
Check out the Crimson project updates blog post for Tentacle
where we highlight some of the work included in the latest release, moving us
closer to fully replacing the existing Classical OSD in the future: 
https://ceph.io/en/news/blog/2025/crimson-T-release/

If you're new to the Crimson project, please visit the project
page for more information and resources: https://ceph.io/en/news/crimson

Dashboard
---------

* There is now added support for NVMe/TCP gateway groups and multiple
  namespaces, multi-cluster management, OAuth 2.0 integration, and enhanced
  RGW/SMB features including multi-site automation, tiering, policies,
  lifecycles, notifications, and granular replication.

MGR
---

* The Ceph Manager's always-on modulues/plugins can now be force-disabled.
  This can be necessary in cases where we wish to prevent the manager from being
  flooded by module commands when Ceph services are down or degraded.

* ``mgr/restful``, ``mgr/zabbix``: both modules, already deprecated since 2020, have been
  finally removed. They have not been actively maintained in the last years,
  and started suffering from vulnerabilities in their dependency chain (e.g.:
  CVE-2023-46136). An alternative for the ``restful`` module is the ``dashboard`` module,
  which provides a richer and better maintained RESTful API. Regarding the ``zabbix`` module,
  there are alternative monitoring solutions, like ``prometheus``, which is the most
  widely adopted among the Ceph user community.

RADOS
-----

* Long-anticipated performance and space amplification optimizations (FastEC)
  are added for erasure-coded pools, including partial reads and partial writes.

* A new implementation of the Erasure Coding I/O code provides substantial
  performance improvements and some capacity improvements. The new code is
  designed to optimize performance when using Erasure Coding with block storage
  (RBD) and file storage (CephFS) but will have benefits for object storage
  (RGW), in particular when using smaller sized objects. A new flag
  ``allow_ec_optimizations`` must be set on each pool to switch to using the
  new code. Existing pools can be upgraded once the OSD and Monitor daemons
  have been updated. There is no need to update the clients.

* The default plugin for erasure coded pools has been changed from Jerasure to
  ISA-L. Clusters created on Tentacle or later releases will use ISA-L as the
  default plugin when creating a new pool. Clusters that upgrade to the T release
  will continue to use their existing default values. The default values can be
  overridden by creating a new erasure code profile and selecting it when
  creating a new pool. ISA-L is recommended for new pools because the Jerasure
  library is no longer maintained.

* BlueStore now has better compression and a new, faster WAL (write-ahead-log).

* All components have been switched to the faster OMAP iteration interface, which
  improves RGW bucket listing and scrub operations.

* It is now possible to bypass ``ceph_assert()`` in extreme cases to help with
  disaster recovery.

* Testing improvements for dencoding verification were added.

* A new command, ``ceph osd pool availability-status``, has been added that
  allows users to view the availability score for each pool in a cluster. A pool
  is considered unavailable if any PG in the pool is not ``active`` or if
  there are unfound objects. Otherwise the pool is considered available. The
  score is updated every one second by default. This interval can be changed
  using the new config option ``pool_availability_update_interval``. The feature
  is off by default. A new config option ``enable_availability_tracking`` can be
  used to turn on the feature if required. Another command is added to clear the
  availability status for a specific pool:

  ::

    ceph osd pool clear-availability-status <pool-name>

  This feature is in tech preview.

  Related links:

  - Feature ticket: https://tracker.ceph.com/issues/67777
  - Documentation: :ref:`data_availability_score`

* Leader monitor and stretch mode status are now included in the ``ceph status``
  output.

  Related tracker: https://tracker.ceph.com/issues/70406

* The ``ceph df`` command reports incorrect ``MAX AVAIL`` for stretch mode pools
  when CRUSH rules use multiple take steps for datacenters. ``PGMap::get_rule_avail``
  incorrectly calculates available space from only one datacenter. As a workaround,
  define CRUSH rules with ``take default`` and ``choose firstn 0 type datacenter``.
  See https://tracker.ceph.com/issues/56650#note-6 for details.

  Upgrading a cluster configured with a CRUSH rule with multiple take steps can
  lead to data shuffling, as the new CRUSH changes may necessitate data
  redistribution. In contrast, a stretch rule with a single-take configuration
  will not cause any data movement during the upgrade process.

* Added convenience function ``librados::AioCompletion::cancel()`` with the same
  behavior as ``librados::IoCtx::aio_cancel()``.

* The configuration parameter ``osd_repair_during_recovery`` has been removed.
  That configuration flag used to control whether an operator-initiated "repair
  scrub" would be allowed to start on an OSD that is performing a recovery. In
  this Ceph version, operator-initiated scrubs and repair scrubs are never blocked
  by a repair being performed.

* Fixed issue of recovery/backfill hang due to improper handling of items in the
  dmclock's background clean-up thread.

  Related tracker: https://tracker.ceph.com/issues/61594

* The OSD's IOPS capacity used by the mClock scheduler is now also checked to
  determine if it's below a configured threshold value defined by:

  - ``osd_mclock_iops_capacity_low_threshold_hdd`` – set to 50 IOPS
  - ``osd_mclock_iops_capacity_low_threshold_ssd`` – set to 1000 IOPS

  The check is intended to handle cases where the measured IOPS is unrealistically
  low. If such a case is detected, the IOPS capacity is either set to the last
  valid value or the configured default to avoid affecting cluster performance
  (slow or stalled ops).

* Documentation has been updated with steps to override OSD IOPS capacity
  configuration.

  Related links:

  - Tracker ticket: https://tracker.ceph.com/issues/70774
  - Documentation: :ref:`override_max_iops_capacity`

* pybind/rados: Fixes ``WriteOp.zero()`` in the original reversed order of arguments
  ``offset`` and ``length``. When pybind calls ``WriteOp.zero()``, the argument passed
  does not match ``rados_write_op_zero``, and offset and length are swapped, which
  results in an unexpected response.

RBD
---

* RBD images can now be instantly imported from another Ceph cluster. The
  migration source spec for ``native`` format has grown ``cluster_name`` and
  ``client_name`` optional fields for connecting to the source cluster after
  parsing the respective ``ceph.conf``-like configuration file.

* With the help of the new NBD stream (``"type": "nbd"``), RBD images can now
  be instantly imported from a wide variety of external sources/formats. The
  exact set of supported formats and their features depends on the capabilities
  of the NBD server.

* While mirroring between Ceph clusters, the local and remote RBD namespaces
  don't need to be the same anymore (but the pool names still do). Using the
  new ``--remote-namespace`` option of ``rbd mirror pool enable`` command, it's
  now possible to pair a local namespace with an arbitrary remote namespace in
  the respective pool, including mapping a default namespace to a non-default
  namespace and vice versa, at the time mirroring is configured.

* All Python APIs that produce timestamps now return "aware" ``datetime``
  objects instead of "naive" ones (i.e., those including time zone information
  instead of those not including it). All timestamps remain in UTC, but
  including ``timezone.utc`` makes it explicit and avoids the potential of the
  returned timestamp getting misinterpreted. In Python 3, many ``datetime``
  methods treat "naive" ``datetime`` objects as local times.

* ``rbd group info`` and ``rbd group snap info`` commands are introduced to
  show information about a group and a group snapshot respectively.

* ``rbd group snap ls`` output now includes the group snapshot IDs. The header
  of the column showing the state of a group snapshot in the unformatted CLI
  output is changed from ``STATUS`` to ``STATE``. The state of a group snapshot
  that was shown as ``ok`` is now shown as ``complete``, which is more
  descriptive.

* In ``rbd mirror image status`` and ``rbd mirror pool status --verbose``
  outputs, ``mirror_uuids`` field has been renamed to ``mirror_uuid`` to
  highlight that the value is always a single UUID and never a list of any
  kind.

* Moving an image that is a member of a group to trash is no longer
  allowed. The ``rbd trash mv`` command now behaves the same way as ``rbd rm``
  in this scenario.

* ``rbd device map`` command now defaults to ``msgr2`` for all device types.
  ``-o ms_mode=legacy`` can be passed to continue using ``msgr1`` with krbd.

* The family of diff-iterate APIs has been extended to allow diffing from or
  between non-user type snapshots which can only be referred to by their IDs.

* Fetching the mirroring mode of an image is invalid if the image is
  disabled for mirroring. The public APIs -- C++ ``mirror_image_get_mode()``,
  C ``rbd_mirror_image_get_mode()``, and Python ``Image.mirror_image_get_mode()``
  -- will return ``EINVAL`` when mirroring is disabled.

* Promoting an image is invalid if the image is not enabled for mirroring.
  The public APIs -- C++ ``mirror_image_promote()``,
  C ``rbd_mirror_image_promote()``, and Python ``Image.mirror_image_promote()``
  -- will return EINVAL instead of ENOENT when mirroring is not enabled.

* Requesting a resync on an image is invalid if the image is not enabled
  for mirroring. The public APIs -- C++ ``mirror_image_resync()``,
  C ``rbd_mirror_image_resync()``, and Python ``Image.mirror_image_resync()``
  -- will return EINVAL instead of ENOENT when mirroring is not enabled.

RGW
---

* Multiple fixes: Lua scripts will no longer run uselessly against health checks,
  properly quoted ``ETag`` values returned by S3 ``CopyPart``, ``PostObject``, and
  ``CompleteMultipartUpload`` responses.

* IAM policy evaluation now supports conditions ``ArnEquals`` and ``ArnLike``,
  along with their ``Not`` and ``IfExists`` variants.

* Added BEAST frontend option ``so_reuseport`` which facilitates running multiple
  RGW instances on the same host by sharing a single TCP port.

* Replication policies now validate permissions using
  ``s3:ReplicateObject``, ``s3:ReplicateDelete``, and ``s3:ReplicateTags`` for
  destination buckets. For source buckets, both
  ``s3:GetObjectVersionForReplication`` and ``s3:GetObject(Version)`` are
  supported. Actions like ``s3:GetObjectAcl``, ``s3:GetObjectLegalHold``, and
  ``s3:GetObjectRetention`` are also considered when fetching the source object.
  Replication of tags is controlled by the
  ``s3:GetObject(Version)Tagging`` permission.

* Adding missing quotes to the ``ETag`` values returned by S3 ``CopyPart``,
  ``PostObject``, and ``CompleteMultipartUpload`` responses.

* ``PutObjectLockConfiguration`` can now be used to enable S3 Object Lock on an
  existing versioning-enabled bucket that was not created with Object Lock enabled.

* The ``x-amz-confirm-remove-self-bucket-access`` header is now supported by
  ``PutBucketPolicy``. Additionally, the root user will always have access to
  modify the bucket policy, even if the current policy explicitly denies access.

* Added support for the ``RestrictPublicBuckets`` property of the S3
  ``PublicAccessBlock`` configuration.

* The HeadBucket API now reports the ``X-RGW-Bytes-Used`` and ``X-RGW-Object-Count``
  headers only when the ``read-stats`` querystring is explicitly included in the
  API request.

Telemetry
---------

* The ``basic`` channel in telemetry now captures the ``ec_optimizations``
  flag, which will allow us to gauge feature adoption for the new
  FastEC improvements.
  To opt into telemetry, run ``ceph telemetry on``.

Upgrading from Reef or Squid
----------------------------

Before starting, ensure that your cluster is stable and healthy with no
``down``, ``recovering``, ``incomplete``, ``undersized`` or ``backfilling`` PGs.
You can temporarily disable the PG autoscaler for all pools during the upgrade
by running ``ceph osd pool set noautoscale`` before beginning, and if the
autoscaler is desired after completion, running ``ceph osd pool unset
noautoscale`` after upgrade success is confirmed.

.. note::

   You can monitor the progress of your upgrade at each stage with the ``ceph versions`` command, which will tell you what Ceph version(s) are running for each type of daemon.

Upgrading Cephadm Clusters
--------------------------

If your cluster is deployed with cephadm (first introduced in Octopus), then the upgrade process is entirely automated. To initiate the upgrade,

.. prompt:: bash #

    ceph orch upgrade start --image quay.io/ceph/ceph:v20.2.0

The same process is used to upgrade to future minor releases.

Upgrade progress can be monitored with

.. prompt:: bash #

    ceph orch upgrade status

Upgrade progress can also be monitored with ``ceph -s`` (which provides a simple progress bar) or more verbosely with

.. prompt:: bash #

    ceph -W cephadm

The upgrade can be paused or resumed with

.. prompt:: bash #

    ceph orch upgrade pause  # to pause
    ceph orch upgrade resume # to resume

or canceled with

.. prompt:: bash #

    ceph orch upgrade stop

Note that canceling the upgrade simply stops the process. There is no ability to downgrade back to Reef or Squid.

Upgrading Non-cephadm Clusters
------------------------------

.. note::

   1. If your cluster is running Reef (18.2.x) or later, you might choose
      to first convert it to use cephadm so that the upgrade to Tentacle is automated (see above).
      For more information, see https://docs.ceph.com/en/tentacle/cephadm/adoption/.

   2. If your cluster is running Reef (18.2.x) or later, systemd unit file
      names have changed to include the cluster fsid. To find the correct
      systemd unit file name for your cluster, run following command:

      ::

        systemctl -l | grep <daemon type>

      Example:

      .. prompt:: bash $

        systemctl -l | grep mon | grep active

      ::

        ceph-6ce0347c-314a-11ee-9b52-000af7995d6c@mon.f28-h21-000-r630.service                                           loaded active running   Ceph mon.f28-h21-000-r630 for 6ce0347c-314a-11ee-9b52-000af7995d6c

#. Set the ``noout`` flag for the duration of the upgrade. (Optional, but recommended.)

   .. prompt:: bash #

      ceph osd set noout

#. Upgrade Monitors by installing the new packages and restarting the Monitor daemons. For example, on each Monitor host

   .. prompt:: bash #

      systemctl restart ceph-mon.target

   Once all Monitors are up, verify that the Monitor upgrade is complete by looking for the ``tentacle`` string in the mon map. The command

   .. prompt:: bash #

      ceph mon dump | grep min_mon_release

   should report:

   .. prompt:: bash #

      min_mon_release 20 (tentacle)

   If it does not, that implies that one or more Monitors haven't been upgraded and restarted and/or the quorum does not include all Monitors.

#. Upgrade ``ceph-mgr`` daemons by installing the new packages and restarting all Manager daemons. For example, on each Manager host,

   .. prompt:: bash #

      systemctl restart ceph-mgr.target

   Verify the ``ceph-mgr`` daemons are running by checking ``ceph -s``:

   .. prompt:: bash #

      ceph -s

   ::

     ...
       services:
        mon: 3 daemons, quorum foo,bar,baz
        mgr: foo(active), standbys: bar, baz
     ...

#. Upgrade all OSDs by installing the new packages and restarting the ``ceph-osd`` daemons on all OSD hosts

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

   #. Restore the original value of ``max_mds`` for the volume

      .. prompt:: bash #

         ceph fs set <fs_name> max_mds <original_max_mds>

#. Upgrade all ``radosgw`` daemons by upgrading packages and restarting daemons on all hosts

   .. prompt:: bash #

      systemctl restart ceph-radosgw.target

#. Complete the upgrade by disallowing pre-Tentacle OSDs and enabling all new Tentacle-only functionality

   .. prompt:: bash #

      ceph osd require-osd-release tentacle

#. If you set ``noout`` at the beginning, be sure to clear it with

   .. prompt:: bash #

      ceph osd unset noout

#. Consider transitioning your cluster to use the cephadm deployment and orchestration framework to simplify
   cluster management and future upgrades. For more information on converting an existing cluster to cephadm,
   see :ref:`cephadm-adoption`.

Post-upgrade
------------

#. Verify the cluster is healthy with ``ceph health``.

#. Consider enabling the :ref:`telemetry` to send anonymized usage statistics
   and crash information to Ceph upstream developers. To see what would
   be reported without actually sending any information to anyone,

   .. prompt:: bash #

      ceph telemetry preview-all

   If you are comfortable with the data that is reported, you can opt-in to automatically report high-level cluster metadata with

   .. prompt:: bash #

      ceph telemetry on

   The public dashboard that aggregates Ceph telemetry can be found at https://telemetry-public.ceph.com/.

Upgrading from Pre-Reef Releases (like Quincy)
----------------------------------------------

You **must** first upgrade to Reef (18.2.z) or Squid (19.2.z) before upgrading to Tentacle.
