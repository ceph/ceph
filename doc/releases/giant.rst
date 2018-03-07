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

For more detailed information, see :download:`the complete changelog <../changelog/v0.87.2.txt>`.

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

For more detailed information, see :download:`the complete changelog <../changelog/v0.87.1.txt>`.



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
