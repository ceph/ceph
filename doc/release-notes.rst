===============
 Release Notes
===============

v0.80 Firefly (upcoming release, draft notes)
=============================================

Upgrade Sequencing
------------------

* If your existing cluster is running a version older than v0.67
  Dumpling, please first upgrade to the latest Dumpling release before
  upgrading to v0.80 Firefly.  Please refer to the :ref:`Dumpling upgrade`
  documentation.

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

TBD


* A librados WATCH operation on a non-existent object now returns ENOENT;
  previously it did not.

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

* The default CRUSH rules and layouts are now using the latest and
  greatest tunables and defaults.  Clusters using the old values will
  now present with a health WARN state.  This can be disabled by
  adding 'mon warn on legacy crush tunables = false' to ceph.conf.

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

TBD


Notable changes since v0.72 Emperor
-----------------------------------

* buffer: some zero-copy groundwork (Josh Durgin)
* build: misc improvements (Ken Dreyer)
* ceph-crush-location: new hook for setting CRUSH location of osd daemons on start)
* ceph-disk: avoid fd0 (Loic Dachary)
* ceph-disk: generalize path names, add tests (Loic Dachary)
* ceph-disk: misc improvements for puppet (Loic Dachary)
* ceph-disk: several bug fixes (Loic Dachary)
* ceph-fuse: fix race for sync reads (Sage Weil)
* ceph-kvstore-tool: expanded command set and capabilities (Joao Eduardo Luis)
* ceph.spec: fix build dependency (Loic Dachary)
* common: bloom filter improvements (Sage Weil)
* common: check preexisting admin socket for active daemon before removing (Loic Dachary)
* common: fix aligned buffer allocation (Loic Dachary)
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
* debian: integrate misc fixes from downstream packaging (James Page)
* doc: big update to install docs (John Wilkins)
* doc: many many install doc improvements (John Wilkins)
* doc: many many updates (John Wilkins)
* doc: misc fixes (David Moreau Simard, Kun Huang)
* erasure-code: improve buffer alignment (Loic Dachary)
* erasure-code: rewrite region-xor using vector operations (Andreas Peters)
* libcephfs: fix resource leak (Zheng Yan)
* librados: add C API coverage for atomic write operations (Christian Marie)
* librados: fix throttle leak (and eventual deadlock) (Josh Durgin)
* librados, osd: new TMAP2OMAP operation (Yan, Zheng)
* librados: read directly into user buffer (Rutger ter Borg)
* librbd: fix use-after-free aio completion bug #5426 (Josh Durgin)
* librbd: localize/distribute parent reads (Sage Weil)
* mailmap: affiliation updates (Loic Dachary)
* mailmap updates (Loic Dachary)
* many portability improvements (Noah Watkins)
* many unit test improvements (Loic Dachary)
* mds: always store backtrace in default pool (Yan, Zheng)
* mds: fix cap migration behavior (Yan, Zheng)
* mds: fix client session flushing (Yan, Zheng)
* mds: fix many many multi-mds bugs (Yan, Zheng)
* mds: fix readdir end check (Zheng Yan)
* mds: fix Resetter locking (Alexandre Oliva)
* mds: inline data support (Li Wang, Yunchuan Wen)
* mds: store directories in omap instead of tmap (Yan, Zheng)
* mds: update old-format backtraces opportunistically (Zheng Yan)
* misc cleanups from coverity (Xing Lin)
* misc coverity fixes (Xing Lin, Li Wang, Danny Al-Gaaf)
* misc portability fixes (Noah Watkins, Alan Somers)
* misc portability fixes (Noah Watkins, Christophe Courtaut, Alan Somers, huanjun)
* misc portability work (Noah Watkins)
* mon: add 'mon getmap EPOCH' (Joao Eduardo Luis)
* mon: allow adjustment of cephfs max file size via 'ceph mds set max_file_size' (Sage Weil)
* mon: allow debug quorum_{enter,exit} commands via admin socket
* mon: change mds allow_new_snaps syntax to be more consistent (Sage Weil)
* mon: clean up initial crush rule creation (Loic Dachary)
* mon: collect misc metadata about osd (os, kernel, etc.), new 'osd metadata' command (Sage Weil)
* mon: do not create erasure rules by default (Sage Weil)
* mon: do not generate spurious MDSMaps in certain cases (Sage Weil)
* mon: do not use keyring if auth = none (Loic Dachary)
* mon: fix pg_temp leaks (Joao Eduardo Luis)
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
* mon: take 'osd pool set ...' value as an int, not string (Joao Eduardo Luis)
* mon: track osd features in OSDMap (Joao Luis, David Zafman)
* mon: trim MDSMaps (Joao Eduardo Luis)
* mon: warn if crush has non-optimal tunables (Sage Weil)
* mount.ceph: add -n for autofs support (Steve Stock)
* msgr: fix messenger restart race (Xihui He)
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
* osd: improve locking in fd lookup cache (Samuel Just, Greg Farnum)
* osd: include more info in pg query result (Sage Weil)
* osd, librados: fix full cluster handling (Josh Durgin)
* osd: new 'chassis' type in default crush hierarchy (Sage Weil)
* osd: new keyvaluestore-dev backend based on leveldb (Haomai Wang)
* osd: new OSDMap encoding (Greg Farnum)
* osd: preliminary cache pool support (no snaps) (Greg Farnum, Sage Weil)
* osd: requery unfound on stray notify (#6909) (Samuel Just)
* osd: some PGBackend infrastructure (Samuel Just)
* osd: support for new 'memstore' (memory-backed) backend (Sage Weil)
* osd: track erasure compatibility (David Zafman)
* rados: add 'crush location', smart replica selection/balancing (Sage Weil)
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
* rgw: convert bucket info to new format on demand (Yehuda Sadeh)
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

Upgrade Sequencing
------------------

.. _Dumpling upgrade:

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




