====
Reef
====

Reef is the 18th stable release of Ceph. It is named after the reef squid
(Sepioteuthis).


v18.2.2 Reef
============

This is a hotfix release that resolves several flaws including Prometheus crashes and an encoder fix.

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

      ```
      systemctl -l | grep <daemon type>
      ```

      Example:

      ```
      $ systemctl -l | grep mon | grep active
      ceph-6ce0347c-314a-11ee-9b52-000af7995d6c@mon.f28-h21-000-r630.service                                           loaded active running   Ceph mon.f28-h21-000-r630 for 6ce0347c-314a-11ee-9b52-000af7995d6c
      ```

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
