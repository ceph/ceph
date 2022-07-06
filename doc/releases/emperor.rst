=======
Emperor
=======

Emperor is the 5th stable release of Ceph.  It is named after the emperor squid.


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
CLI compatibility with mixed version monitors.  (In the future this
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

For more detailed information, see :download:`the complete changelog <../changelog/v0.72.2.txt>`.

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
