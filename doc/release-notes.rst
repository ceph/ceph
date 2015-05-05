===============
 Release Notes
===============

v9.0.0
======

This is the first development release for the Infernalis cycle, and
the first Ceph release to sport a version number from the new
numbering scheme.  The "9" indicates this is the 9th release cycle--I
(for Infernalis) is the 9th letter.  The first "0" indicates this is a
development release ("1" will mean release candidate and "2" will mean
stable release), and the final "0" indicates this is the first such
development release.

A few highlights include:

* a new 'ceph daemonperf' command to watch perfcounter stats in realtime
* reduced MDS memory usage
* many MDS snapshot fixes
* librbd can now store options in the image itself
* many fixes for RGW Swift API support
* OSD performance improvements
* many doc updates and misc bug fixes

Notable Changes
---------------

* aarch64: add optimized version of crc32c (Yazen Ghannam, Steve Capper)
* auth: reinit NSS after fork() (#11128 Yan, Zheng)
* build: disable LTTNG by default (#11333 Josh Durgin)
* build: fix ppc build (James Page)
* build: install-deps: support OpenSUSE (Loic Dachary)
* build: misc cmake fixes (Matt Benjamin)
* ceph-disk: follow ceph-osd hints when creating journal (#9580 Sage Weil)
* ceph-disk: handle re-using existing partition (#10987 Loic Dachary)
* ceph-disk: improve parted output parsing (#10983 Loic Dachary)
* ceph-disk: make suppression work for activate-all and activate-journal (Dan van der Ster)
* ceph-disk: misc fixes (Alfredo Deza)
* ceph-fuse, libcephfs: don't clear COMPLETE when trimming null (Yan, Zheng)
* ceph-fuse, libcephfs: hold exclusive caps on dirs we "own" (#11226 Greg Farnum)
* ceph-fuse: do not require successful remount when unmounting (#10982 Greg Farnum)
* ceph: new 'ceph daemonperf' command (John Spray, Mykola Golub)
* common: PriorityQueue tests (Kefu Chai)
* common: add descriptions to perfcounters (Kiseleva Alyona)
* common: fix LTTNG vs fork issue (Josh Durgin)
* crush: fix has_v4_buckets (#11364 Sage Weil)
* crushtool: fix order of operations, usage (Sage Weil)
* debian: minor package reorg (Ken Dreyer)
* doc: docuemnt object corpus generation (#11099 Alexis Normand)
* doc: fix gender neutrality (Alexandre Maragone)
* doc: fix install doc (#10957 Kefu Chai)
* doc: fix sphinx issues (Kefu Chai)
* doc: mds data structure docs (Yan, Zheng)
* doc: misc updates (Nilamdyuti Goswami, Vartika Rai, Florian Haas, Loic Dachary, Simon Guinot, Andy Allan, Alistair Israel, Ken Dreyer, Robin Rehu, Lee Revell, Florian Marsylle, Thomas Johnson, Bosse Klykken, Travis Rhoden, Ian Kelling)
* doc: swift tempurls (#10184 Abhishek Lekshmanan)
* doc: switch doxygen integration back to breathe (#6115 Kefu Chai)
* erasure-code: update ISA-L to 2.13 (Yuan Zhou)
* gmock: switch to submodule (Danny Al-Gaaf, Loic Dachary)
* hadoop: add terasort test (Noah Watkins)
* java: fix libcephfs bindings (Noah Watkins)
* libcephfs,ceph-fuse: fix request resend on cap reconnect (#10912 Yan, Zheng)
* librados: define C++ flags from C constants (Josh Durgin)
* librados: fix last_force_resent handling (#11026 Jianpeng Ma)
* librados: fix memory leak from C_TwoContexts (Xiong Yiliang)
* librados: fix striper when stripe_count = 1 and stripe_unit != object_size (#11120 Yan, Zheng)
* librados: op perf counters (John Spray)
* librados: pybind: fix write() method return code (Javier Guerra)
* libradosstriper: fix leak (Danny Al-Gaaf)
* librbd: add purge_on_error cache behavior (Jianpeng Ma)
* librbd: misc aio fixes (#5488 Jason Dillaman)
* librbd: misc rbd fixes (#11478 #11113 #11342 #11380 Jason Dillaman, Zhiqiang Wang)
* librbd: readahead fixes (Zhiqiang Wang)
* librbd: store metadata, including config options, in image (Haomai Wang)
* mds: add 'damaged' state to MDSMap (John Spray)
* mds: add nicknames for perfcounters (John Spray)
* mds: disable problematic rstat propagation into snap parents (Yan, Zheng)
* mds: fix mydir replica issue with shutdown (#10743 John Spray)
* mds: fix out-of-order messages (#11258 Yan, Zheng)
* mds: fix shutdown with strays (#10744 John Spray)
* mds: fix snapshot fixes (Yan, Zheng)
* mds: fix stray handling (John Spray)
* mds: flush immediately in do_open_truncate (#11011 John Spray)
* mds: improve dump methods (John Spray)
* mds: misc journal cleanups and fixes (#10368 John Spray)
* mds: new SessionMap storage using omap (#10649 John Spray)
* mds: reduce memory consumption (Yan, Zheng)
* mds: throttle purge stray operations (#10390 John Spray)
* mds: tolerate clock jumping backwards (#11053 Yan, Zheng)
* misc coverity fixes (Danny Al-Gaaf)
* mon: do not deactivate last mds (#10862 John Spray)
* mon: make osd get pool 'all' only return applicable fields (#10891 Michal Jarzabek)
* mon: warn on bogus cache tier config (Jianpeng Ma)
* msg/async: misc bug fixes and updates (Haomai Wang)
* msg/simple: fix connect_seq assert (Haomai Wang)
* msg/xio: misc fixes (#10735 Matt Benjamin, Kefu Chai, Danny Al-Gaaf, Raju Kurunkad, Vu Pham)
* msg: unit tests (Haomai Wang)
* objectcacher: misc bug fixes (Jianpeng Ma)
* os/filestore: enlarge getxattr buffer size (Jianpeng Ma)
* osd: EIO injection (David Zhang)
* osd: add misc perfcounters (Xinze Chi)
* osd: add simple sleep injection in recovery (Sage Weil)
* osd: allow SEEK_HOLE/SEEK_DATA for sparse read (Zhiqiang Wang)
* osd: avoid dup omap sets for in pg metadata (Sage Weil)
* osd: clean up some constness, privateness (Kefu Chai)
* osd: erasure-code: drop entries according to LRU (Andreas-Joachim Peters)
* osd: fix negative degraded stats during backfill (Guang Yang)
* osd: misc fixes (Ning Yao, Kefu Chai, Xinze Chi, Zhiqiang Wang, Jianpeng Ma)
* pybind: pep8 cleanups (Danny Al-Gaaf)
* qa: fix filelock_interrupt.py test (Yan, Zheng)
* qa: improve ceph-disk tests (Loic Dachary)
* qa: improve docker build layers (Loic Dachary)
* rados: translate erno to string in CLI (#10877 Kefu Chai)
* rbd: accept map options config option (Ilya Dryomov)
* rbd: cli: fix arg parsing with --io-pattern (Dmitry Yatsushkevich)
* rbd: fix error messages (#2862 Rajesh Nambiar)
* rbd: update rbd man page (Ilya Dryomov)
* rbd: update xfstests tests (Douglas Fuller)
* rgw: add X-Timestamp for Swift containers (#10938 Radoslaw Zarzynski)
* rgw: add missing headers to Swift container details (#10666 Ahmad Faheem, Dmytro Iurchenko)
* rgw: add stats to headers for account GET (#10684 Yuan Zhou)
* rgw: do not prefecth data for HEAD requests (Guang Yang)
* rgw: don't clobber bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: don't use rgw_socket_path if frontend is configured (#11160 Yehuda Sadeh)
* rgw: enforce Content-Lenth for POST on Swift cont/obj (#10661 Radoslaw Zarzynski)
* rgw: fix handling empty metadata items on Swift container (#11088 Radoslaw Zarzynski)
* rgw: fix log rotation (Wuxingyi)
* rgw: generate Date header for civetweb (#10873 Radoslaw Zarzynski)
* rgw: make init script wait for radosgw to stop (#11140 Dmitry Yatsushkevich)
* rgw: make quota/gc threads configurable (#11047 Guang Yang)
* rgw: pass in civetweb configurables (#10907 Yehuda Sadeh)
* rgw: rectify 202 Accepted in PUT response (#11148 Radoslaw Zarzynski)
* rgw: remove meta file after deleting bucket (#11149 Orit Wasserman)
* rgw: swift: allow setting attributes with COPY (#10662 Ahmad Faheem, Dmytro Iurchenko)
* rgw: swift: fix metadata handling on copy (#10645 Radoslaw Zarzynski)
* rgw: swift: send Last-Modified header (#10650 Radoslaw Zarzynski)
* rgw: update keystone cache with token info (#11125 Yehuda Sadeh)
* rgw: update to latest civetweb, enable config for IPv6 (#10965 Yehuda Sadeh)
* rocksdb: update to latest (Xiaoxi Chen)
* rpm: loosen ceph-test dependencies (Ken Dreyer)



v0.94.1 Hammer
==============

This bug fix release fixes a few critical issues with CRUSH.  The most
important addresses a bug in feature bit enforcement that may prevent
pre-hammer clients from communicating with the cluster during an
upgrade.  This only manifests in some cases (for example, when the
'rack' type is in use in the CRUSH map, and possibly other cases), but for
safety we strongly recommend that all users use 0.94.1 instead of 0.94 when
upgrading.

There is also a fix in the new straw2 buckets when OSD weights are 0.

We recommend that all v0.94 users upgrade.

Notable changes
---------------

* crush: fix divide-by-0 in straw2 (#11357 Sage Weil)
* crush: fix has_v4_buckets (#11364 Sage Weil)
* osd: fix negative degraded objects during backfilling (#7737 Guang Yang)

For more detailed information, see :download:`the complete changelog <changelog/v0.94.1.txt>`.


v0.94 Hammer
============

This major release is expected to form the basis of the next long-term
stable series.  It is intended to supersede v0.80.x Firefly.

Highlights since Giant include:

* *RADOS Performance*: a range of improvements have been made in the
  OSD and client-side librados code that improve the throughput on
  flash backends and improve parallelism and scaling on fast machines.
* *Simplified RGW deployment*: the ceph-deploy tool now has a new
  'ceph-deploy rgw create HOST' command that quickly deploys a
  instance of the S3/Swift gateway using the embedded Civetweb server.
  This is vastly simpler than the previous Apache-based deployment.
  There are a few rough edges (e.g., around SSL support) but we
  encourage users to try `the new method`_.
* *RGW object versioning*: RGW now supports the S3 object versioning
  API, which preserves old version of objects instead of overwriting
  them.
* *RGW bucket sharding*: RGW can now shard the bucket index for large
  buckets across, improving performance for very large buckets.
* *RBD object maps*: RBD now has an object map function that tracks
  which parts of the image are allocating, improving performance for
  clones and for commands like export and delete.
* *RBD mandatory locking*: RBD has a new mandatory locking framework
  (still disabled by default) that adds additional safeguards to
  prevent multiple clients from using the same image at the same time.
* *RBD copy-on-read*: RBD now supports copy-on-read for image clones,
  improving performance for some workloads.
* *CephFS snapshot improvements*: Many many bugs have been fixed with
  CephFS snapshots.  Although they are still disabled by default,
  stability has improved significantly.
* *CephFS Recovery tools*: We have built some journal recovery and
  diagnostic tools. Stability and performance of single-MDS systems is
  vastly improved in Giant, and more improvements have been made now
  in Hammer.  Although we still recommend caution when storing
  important data in CephFS, we do encourage testing for non-critical
  workloads so that we can better guage the feature, usability,
  performance, and stability gaps.
* *CRUSH improvements*: We have added a new straw2 bucket algorithm
  that reduces the amount of data migration required when changes are
  made to the cluster.
* *Shingled erasure codes (SHEC)*: The OSDs now have experimental
  support for shingled erasure codes, which allow a small amount of
  additional storage to be traded for improved recovery performance.
* *RADOS cache tiering*: A series of changes have been made in the
  cache tiering code that improve performance and reduce latency.
* *RDMA support*: There is now experimental support the RDMA via the
  Accelio (libxio) library.
* *New administrator commands*: The 'ceph osd df' command shows
  pertinent details on OSD disk utilizations.  The 'ceph pg ls ...'
  command makes it much simpler to query PG states while diagnosing
  cluster issues.

.. _the new method: ../start/quick-ceph-deploy/#add-an-rgw-instance

Other highlights since Firefly include:

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
* *Recovery tools*: the ceph-objectstore-tool is greatly expanded to
  allow manipulation of an individual OSDs data store for debugging
  and repair purposes.  This is most heavily used by our QA
  infrastructure to exercise recovery code.

I would like to take this opportunity to call out the amazing growth
in contributors to Ceph beyond the core development team from Inktank.
Hammer features major new features and improvements from Intel, Fujitsu,
UnitedStack, Yahoo, UbuntuKylin, CohortFS, Mellanox, CERN, Deutsche
Telekom, Mirantis, and SanDisk.

Dedication
----------

This release is dedicated in memoriam to Sandon Van Ness, aka
Houkouonchi, who unexpectedly passed away a few weeks ago.  Sandon was
responsible for maintaining the large and complex Sepia lab that
houses the Ceph project's build and test infrastructure.  His efforts
have made an important impact on our ability to reliably test Ceph
with a relatively small group of people.  He was a valued member of
the team and we will miss him.  H is also for Houkouonchi.

Upgrading
---------

* If your existing cluster is running a version older than v0.80.x
  Firefly, please first upgrade to the latest Firefly release before
  moving on to Giant.  We have not tested upgrades directly from
  Emperor, Dumpling, or older releases.

  We *have* tested:

   * Firefly to Hammer
   * Giant to Hammer
   * Dumpling to Firefly to Hammer

* Please upgrade daemons in the following order:

   #. Monitors
   #. OSDs
   #. MDSs and/or radosgw

  Note that the relative ordering of OSDs and monitors should not matter, but
  we primarily tested upgrading monitors first.

* The ceph-osd daemons will perform a disk-format upgrade improve the
  PG metadata layout and to repair a minor bug in the on-disk format.
  It may take a minute or two for this to complete, depending on how
  many objects are stored on the node; do not be alarmed if they do
  not marked "up" by the cluster immediately after starting.

* If upgrading from v0.93, set
   osd enable degraded writes = false

  on all osds prior to upgrading.  The degraded writes feature has
  been reverted due to 11155.

* The LTTNG tracing in librbd and librados is disabled in the release packages
  until we find a way to avoid violating distro security policies when linking
  libust.

Upgrading from v0.80.x Giant
----------------------------

* librbd and librados include lttng tracepoints on distros with
  liblttng 2.4 or later (only Ubuntu Trusty for the ceph.com
  packages). When running a daemon that uses these libraries, i.e. an
  application that calls fork(2) or clone(2) without exec(3), you must
  set LD_PRELOAD=liblttng-ust-fork.so.0 to prevent a crash in the
  lttng atexit handler when the process exits. The only ceph tool that
  requires this is rbd-fuse.

* If rgw_socket_path is defined and rgw_frontends defines a
  socket_port and socket_host, we now allow the rgw_frontends settings
  to take precedence.  This change should only affect users who have
  made non-standard changes to their radosgw configuration.

* If you are upgrading specifically from v0.92, you must stop all OSD
  daemons and flush their journals (``ceph-osd -i NNN
  --flush-journal``) before upgrading.  There was a transaction
  encoding bug in v0.92 that broke compatibility.  Upgrading from v0.93,
  v0.91, or anything earlier is safe.

* The experimental 'keyvaluestore-dev' OSD backend has been renamed
  'keyvaluestore' (for simplicity) and marked as experimental.  To
  enable this untested feature and acknowledge that you understand
  that it is untested and may destroy data, you need to add the
  following to your ceph.conf::

    enable experimental unrecoverable data corrupting featuers = keyvaluestore

* The following librados C API function calls take a 'flags' argument whose value
  is now correctly interpreted:

     rados_write_op_operate()
     rados_aio_write_op_operate()
     rados_read_op_operate()
     rados_aio_read_op_operate()

  The flags were not correctly being translated from the librados constants to the
  internal values.  Now they are.  Any code that is passing flags to these methods
  should be audited to ensure that they are using the correct LIBRADOS_OP_FLAG_*
  constants.

* The 'rados' CLI 'copy' and 'cppool' commands now use the copy-from operation,
  which means the latest CLI cannot run these commands against pre-firefly OSDs.

* The librados watch/notify API now includes a watch_flush() operation to flush
  the async queue of notify operations.  This should be called by any watch/notify
  user prior to rados_shutdown().

* The 'category' field for objects has been removed.  This was originally added
  to track PG stat summations over different categories of objects for use by
  radosgw.  It is no longer has any known users and is prone to abuse because it
  can lead to a pg_stat_t structure that is unbounded.  The librados API calls
  that accept this field now ignore it, and the OSD no longers tracks the
  per-category summations.

* The output for 'rados df' has changed.  The 'category' level has been
  eliminated, so there is now a single stat object per pool.  The structure of
  the JSON output is different, and the plaintext output has one less column.

* The 'rados create <objectname> [category]' optional category argument is no
  longer supported or recognized.

* rados.py's Rados class no longer has a __del__ method; it was causing
  problems on interpreter shutdown and use of threads.  If your code has
  Rados objects with limited lifetimes and you're concerned about locked
  resources, call Rados.shutdown() explicitly.

* There is a new version of the librados watch/notify API with vastly
  improved semantics.  Any applications using this interface are
  encouraged to migrate to the new API.  The old API calls are marked
  as deprecated and will eventually be removed.

* The librados rados_unwatch() call used to be safe to call on an
  invalid handle.  The new version has undefined behavior when passed
  a bogus value (for example, when rados_watch() returns an error and
  handle is not defined).

* The structure of the formatted 'pg stat' command is changed for the
  portion that counts states by name to avoid using the '+' character
  (which appears in state names) as part of the XML token (it is not
  legal).

* Previously, the formatted output of 'ceph pg stat -f ...' was a full
  pg dump that included all metadata about all PGs in the system.  It
  is now a concise summary of high-level PG stats, just like the
  unformatted 'ceph pg stat' command.

* All JSON dumps of floating point values were incorrecting surrounding the
  value with quotes.  These quotes have been removed.  Any consumer of structured
  JSON output that was consuming the floating point values was previously having
  to interpret the quoted string and will most likely need to be fixed to take
  the unquoted number.

* New ability to list all objects from all namespaces can fail or
  return incomplete results when not all OSDs have been upgraded.
  Features rados --all ls, rados cppool, rados export, rados
  cache-flush-evict-all and rados cache-try-flush-evict-all can also
  fail or return incomplete results.

* Due to a change in the Linux kernel version 3.18 and the limits of the FUSE
  interface, ceph-fuse needs be mounted as root on at least some systems. See
  issues #9997, #10277, and #10542 for details.

Upgrading from v0.80x Firefly (additional notes)
------------------------------------------------

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


Notable changes since v0.93
---------------------------

* build: a few cmake fixes (Matt Benjamin)
* build: fix build on RHEL/CentOS 5.9 (Rohan Mars)
* build: reorganize Makefile to allow modular builds (Boris Ranto)
* ceph-fuse: be more forgiving on remount (#10982 Greg Farnum)
* ceph: improve CLI parsing (#11093 David Zafman)
* common: fix cluster logging to default channel (#11177 Sage Weil)
* crush: fix parsing of straw2 buckets (#11015 Sage Weil)
* doc: update man pages (David Zafman)
* librados: fix leak in C_TwoContexts (Xiong Yiliang)
* librados: fix leak in watch/notify path (Sage Weil)
* librbd: fix and improve AIO cache invalidation (#10958 Jason Dillaman)
* librbd: fix memory leak (Jason Dillaman)
* librbd: fix ordering/queueing of resize operations (Jason Dillaman)
* librbd: validate image is r/w on resize/flatten (Jason Dillaman)
* librbd: various internal locking fixes (Jason Dillaman)
* lttng: tracing is disabled until we streamline dependencies (Josh Durgin)
* mon: add bootstrap-rgw profile (Sage Weil)
* mon: do not pollute mon dir with CSV files from CRUSH check (Loic Dachary)
* mon: fix clock drift time check interval (#10546 Joao Eduardo Luis)
* mon: fix units in store stats (Joao Eduardo Luis)
* mon: improve error handling on erasure code profile set (#10488, #11144 Loic Dachary)
* mon: set {read,write}_tier on 'osd tier add-cache ...' (Jianpeng Ma)
* ms: xio: fix misc bugs (Matt Benjamin, Vu Pham)
* osd: DBObjectMap: fix locking to prevent rare crash (#9891 Samuel Just)
* osd: fix and document last_epoch_started semantics (Samuel Just)
* osd: fix divergent entry handling on PG split (Samuel Just)
* osd: fix leak on shutdown (Kefu Chai)
* osd: fix recording of digest on scrub (Samuel Just)
* osd: fix whiteout handling (Sage Weil)
* rbd: allow v2 striping parameters for clones and imports (Jason Dillaman)
* rbd: fix formatted output of image features (Jason Dillaman)
* rbd: updat eman page (Ilya Dryomov)
* rgw: don't overwrite bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: enable IPv6 for civetweb (#10965 Yehuda Sadeh)
* rgw: fix sysvinit script when rgw_socket_path is not defined (#11159 Yehuda Sadeh, Dan Mick)
* rgw: pass civetweb configurables through (#10907 Yehuda Sadeh)
* rgw: use new watch/notify API (Yehuda Sadeh, Sage Weil)
* osd: reverted degraded writes feature due to 11155

Notable changes since v0.87.x Giant
-----------------------------------

* add experimental features option (Sage Weil)
* arch: fix NEON feaeture detection (#10185 Loic Dachary)
* asyncmsgr: misc fixes (Haomai Wang)
* buffer: add 'shareable' construct (Matt Benjamin)
* buffer: add list::get_contiguous (Sage Weil)
* buffer: avoid rebuild if buffer already contiguous (Jianpeng Ma)
* build: CMake support (Ali Maredia, Casey Bodley, Adam Emerson, Marcus Watts, Matt Benjamin)
* build: a few cmake fixes (Matt Benjamin)
* build: aarch64 build fixes (Noah Watkins, Haomai Wang)
* build: adjust build deps for yasm, virtualenv (Jianpeng Ma)
* build: fix 'make check' races (#10384 Loic Dachary)
* build: fix build on RHEL/CentOS 5.9 (Rohan Mars)
* build: fix pkg names when libkeyutils is missing (Pankag Garg, Ken Dreyer)
* build: improve build dependency tooling (Loic Dachary)
* build: reorganize Makefile to allow modular builds (Boris Ranto)
* build: support for jemalloc (Shishir Gowda)
* ceph-disk: Scientific Linux support (Dan van der Ster)
* ceph-disk: allow journal partition re-use (#10146 Loic Dachary, Dav van der Ster)
* ceph-disk: call partx/partprobe consistency (#9721 Loic Dachary)
* ceph-disk: do not re-use partition if encryption is required (Loic Dachary)
* ceph-disk: fix dmcrypt key permissions (Loic Dachary)
* ceph-disk: fix umount race condition (#10096 Blaine Gardner)
* ceph-disk: improved systemd support (Owen Synge)
* ceph-disk: init=none option (Loic Dachary)
* ceph-disk: misc fixes (Christos Stavrakakis)
* ceph-disk: respect --statedir for keyring (Loic Dachary)
* ceph-disk: set guid if reusing journal partition (Dan van der Ster)
* ceph-disk: support LUKS for encrypted partitions (Andrew Bartlett, Loic Dachary)
* ceph-fuse, libcephfs: POSIX file lock support (Yan, Zheng)
* ceph-fuse, libcephfs: allow xattr caps in inject_release_failure (#9800 John Spray)
* ceph-fuse, libcephfs: fix I_COMPLETE_ORDERED checks (#9894 Yan, Zheng)
* ceph-fuse, libcephfs: fix cap flush overflow (Greg Farnum, Yan, Zheng)
* ceph-fuse, libcephfs: fix root inode xattrs (Yan, Zheng)
* ceph-fuse, libcephfs: preserve dir ordering (#9178 Yan, Zheng)
* ceph-fuse, libcephfs: trim inodes before reconnecting to MDS (Yan, Zheng)
* ceph-fuse,libcephfs: add support for O_NOFOLLOW and O_PATH (Greg Farnum)
* ceph-fuse,libcephfs: resend requests before completing cap reconnect (#10912 Yan, Zheng)
* ceph-fuse: be more forgiving on remount (#10982 Greg Farnum)
* ceph-fuse: fix dentry invalidation on 3.18+ kernels (#9997 Yan, Zheng)
* ceph-fuse: fix kernel cache trimming (#10277 Yan, Zheng)
* ceph-fuse: select kernel cache invalidation mechanism based on kernel version (Greg Farnum)
* ceph-monstore-tool: fix shutdown (#10093 Loic Dachary)
* ceph-monstore-tool: fix/improve CLI (Joao Eduardo Luis)
* ceph-objectstore-tool: fix import (#10090 David Zafman)
* ceph-objectstore-tool: improved import (David Zafman)
* ceph-objectstore-tool: many improvements and tests (David Zafman)
* ceph-objectstore-tool: many many improvements (David Zafman)
* ceph-objectstore-tool: misc improvements, fixes (#9870 #9871 David Zafman)
* ceph.spec: package rbd-replay-prep (Ken Dreyer)
* ceph: add 'ceph osd df [tree]' command (#10452 Mykola Golub)
* ceph: do not parse injectargs twice (Loic Dachary)
* ceph: fix 'ceph tell ...' command validation (#10439 Joao Eduardo Luis)
* ceph: improve 'ceph osd tree' output (Mykola Golub)
* ceph: improve CLI parsing (#11093 David Zafman)
* ceph: make 'ceph -s' output more readable (Sage Weil)
* ceph: make 'ceph -s' show PG state counts in sorted order (Sage Weil)
* ceph: make 'ceph tell mon.* version' work (Mykola Golub)
* ceph: new 'ceph tell mds.$name_or_rank_or_gid' (John Spray)
* ceph: show primary-affinity in 'ceph osd tree' (Mykola Golub)
* ceph: test robustness (Joao Eduardo Luis)
* ceph_objectstore_tool: behave with sharded flag (#9661 David Zafman)
* cephfs-journal-tool: add recover_dentries function (#9883 John Spray)
* cephfs-journal-tool: fix journal import (#10025 John Spray)
* cephfs-journal-tool: skip up to expire_pos (#9977 John Spray)
* cleanup rados.h definitions with macros (Ilya Dryomov)
* common: add 'perf reset ...' admin command (Jianpeng Ma)
* common: add TableFormatter (Andreas Peters)
* common: add newline to flushed json output (Sage Weil)
* common: check syncfs() return code (Jianpeng Ma)
* common: do not unlock rwlock on destruction (Federico Simoncelli)
* common: filtering for 'perf dump' (John Spray)
* common: fix Formatter factory breakage (#10547 Loic Dachary)
* common: fix block device discard check (#10296 Sage Weil)
* common: make json-pretty output prettier (Sage Weil)
* common: remove broken CEPH_LOCKDEP optoin (Kefu Chai)
* common: shared_cache unit tests (Cheng Cheng)
* common: support new gperftools header locations (Key Dreyer)
* config: add $cctid meta variable (Adam Crume)
* crush: fix buffer overrun for poorly formed rules (#9492 Johnu George)
* crush: fix detach_bucket (#10095 Sage Weil)
* crush: fix parsing of straw2 buckets (#11015 Sage Weil)
* crush: fix several bugs in adjust_item_weight (Rongze Zhu)
* crush: fix tree bucket behavior (Rongze Zhu)
* crush: improve constness (Loic Dachary)
* crush: new and improved straw2 bucket type (Sage Weil, Christina Anderson, Xiaoxi Chen)
* crush: straw bucket weight calculation fixes (#9998 Sage Weil)
* crush: update tries stats for indep rules (#10349 Loic Dachary)
* crush: use larger choose_tries value for erasure code rulesets (#10353 Loic Dachary)
* crushtool: add --location <id> command (Sage Weil, Loic Dachary)
* debian,rpm: move RBD udev rules to ceph-common (#10864 Ken Dreyer)
* debian: split python-ceph into python-{rbd,rados,cephfs} (Boris Ranto)
* default to libnss instead of crypto++ (Federico Gimenez)
* doc: CephFS disaster recovery guidance (John Spray)
* doc: CephFS for early adopters (John Spray)
* doc: add build-doc guidlines for Fedora and CentOS/RHEL (Nilamdyuti Goswami)
* doc: add dumpling to firefly upgrade section (#7679 John Wilkins)
* doc: ceph osd reweight vs crush weight (Laurent Guerby)
* doc: do not suggest dangerous XFS nobarrier option (Dan van der Ster)
* doc: document erasure coded pool operations (#9970 Loic Dachary)
* doc: document the LRC per-layer plugin configuration (Yuan Zhou)
* doc: enable rbd cache on openstack deployments (Sebastien Han)
* doc: erasure code doc updates (Loic Dachary)
* doc: file system osd config settings (Kevin Dalley)
* doc: fix OpenStack Glance docs (#10478 Sebastien Han)
* doc: improved installation nots on CentOS/RHEL installs (John Wilkins)
* doc: key/value store config reference (John Wilkins)
* doc: misc cleanups (Adam Spiers, Sebastien Han, Nilamdyuti Goswami, Ken Dreyer, John Wilkins)
* doc: misc improvements (Nilamdyuti Goswami, John Wilkins, Chris Holcombe)
* doc: misc updates (#9793 #9922 #10204 #10203 Travis Rhoden, Hazem, Ayari, Florian Coste, Andy Allan, Frank Yu, Baptiste Veuillez-Mainard, Yuan Zhou, Armando Segnini, Robert Jansen, Tyler Brekke, Viktor Suprun)
* doc: misc updates (Alfredo Deza, VRan Liu)
* doc: misc updates (Nilamdyuti Goswami, John Wilkins)
* doc: new man pages (Nilamdyuti Goswami)
* doc: preflight doc fixes (John Wilkins)
* doc: replace cloudfiles with swiftclient Python Swift example (Tim Freund)
* doc: update PG count guide (Gerben Meijer, Laurent Guerby, Loic Dachary)
* doc: update man pages (David Zafman)
* doc: update openstack docs for Juno (Sebastien Han)
* doc: update release descriptions (Ken Dreyer)
* doc: update sepia hardware inventory (Sandon Van Ness)
* erasure-code: add mSHEC erasure code support (Takeshi Miyamae)
* erasure-code: improved docs (#10340 Loic Dachary)
* erasure-code: set max_size to 20 (#10363 Loic Dachary)
* fix cluster logging from non-mon daemons (Sage Weil)
* init-ceph: check for systemd-run before using it (Boris Ranto)
* install-deps.sh: do not require sudo when root (Loic Dachary)
* keyvaluestore: misc fixes (Haomai Wang)
* keyvaluestore: performance improvements (Haomai Wang)
* libcephfs,ceph-fuse: add 'status' asok (John Spray)
* libcephfs,ceph-fuse: fix getting zero-length xattr (#10552 Yan, Zheng)
* libcephfs: fix dirfrag trimming (#10387 Yan, Zheng)
* libcephfs: fix mount timeout (#10041 Yan, Zheng)
* libcephfs: fix test (#10415 Yan, Zheng)
* libcephfs: fix use-afer-free on umount (#10412 Yan, Zheng)
* libcephfs: include ceph and git version in client metadata (Sage Weil)
* librados, osd: new watch/notify implementation (Sage Weil)
* librados: add blacklist_add convenience method (Jason Dillaman)
* librados: add rados_pool_get_base_tier() call (Adam Crume)
* librados: add watch_flush() operation (Sage Weil, Haomai Wang)
* librados: avoid memcpy on getxattr, read (Jianpeng Ma)
* librados: cap buffer length (Loic Dachary)
* librados: create ioctx by pool id (Jason Dillaman)
* librados: do notify completion in fast-dispatch (Sage Weil)
* librados: drop 'category' feature (Sage Weil)
* librados: expose rados_{read|write}_op_assert_version in C API (Kim Vandry)
* librados: fix infinite loop with skipped map epochs (#9986 Ding Dinghua)
* librados: fix iterator operator= bugs (#10082 David Zafman, Yehuda Sadeh)
* librados: fix leak in C_TwoContexts (Xiong Yiliang)
* librados: fix leak in watch/notify path (Sage Weil)
* librados: fix null deref when pool DNE (#9944 Sage Weil)
* librados: fix objecter races (#9617 Josh Durgin)
* librados: fix pool deletion handling (#10372 Sage Weil)
* librados: fix pool name caching (#10458 Radoslaw Zarzynski)
* librados: fix resource leak, misc bugs (#10425 Radoslaw Zarzynski)
* librados: fix some watch/notify locking (Jason Dillaman, Josh Durgin)
* librados: fix timer race from recent refactor (Sage Weil)
* librados: new fadvise API (Ma Jianpeng)
* librados: only export public API symbols (Jason Dillaman)
* librados: remove shadowed variable (Kefu Chain)
* librados: translate op flags from C APIs (Matthew Richards)
* libradosstriper: fix remove() (Dongmao Zhang)
* libradosstriper: fix shutdown hang (Dongmao Zhang)
* libradosstriper: fix stat strtoll (Dongmao Zhang)
* libradosstriper: fix trunc method (#10129 Sebastien Ponce)
* libradosstriper: fix write_full when ENOENT (#10758 Sebastien Ponce)
* libradosstriper: misc fixes (Sebastien Ponce)
* librbd: CRC protection for RBD image map (Jason Dillaman)
* librbd: add missing python docstrings (Jason Dillaman)
* librbd: add per-image object map for improved performance (Jason Dillaman)
* librbd: add readahead (Adam Crume)
* librbd: add support for an "object map" indicating which objects exist (Jason Dillaman)
* librbd: adjust internal locking (Josh Durgin, Jason Dillaman)
* librbd: better handling of watch errors (Jason Dillaman)
* librbd: complete pending ops before closing image (#10299 Josh Durgin)
* librbd: coordinate maint operations through lock owner (Jason Dillaman)
* librbd: copy-on-read (Min Chen, Li Wang, Yunchuan Wen, Cheng Cheng, Jason Dillaman)
* librbd: differentiate between R/O vs R/W features (Jason Dillaman)
* librbd: don't close a closed parent in failure path (#10030 Jason Dillaman)
* librbd: enforce write ordering with a snapshot (Jason Dillaman)
* librbd: exclusive image locking (Jason Dillaman)
* librbd: fadvise API (Ma Jianpeng)
* librbd: fadvise-style hints; add misc hints for certain operations (Jianpeng Ma)
* librbd: fix and improve AIO cache invalidation (#10958 Jason Dillaman)
* librbd: fix cache tiers in list_children and snap_unprotect (Adam Crume)
* librbd: fix coverity false-positives (Jason Dillaman)
* librbd: fix diff test (#10002 Josh Durgin)
* librbd: fix list_children from invalid pool ioctxs (#10123 Jason Dillaman)
* librbd: fix locking for readahead (#10045 Jason Dillaman)
* librbd: fix memory leak (Jason Dillaman)
* librbd: fix ordering/queueing of resize operations (Jason Dillaman)
* librbd: fix performance regression in ObjectCacher (#9513 Adam Crume)
* librbd: fix snap create races (Jason Dillaman)
* librbd: fix write vs import race (#10590 Jason Dillaman)
* librbd: flush AIO operations asynchronously (#10714 Jason Dillaman)
* librbd: gracefully handle deleted/renamed pools (#10270 Jason Dillaman)
* librbd: lttng tracepoints (Adam Crume)
* librbd: make async versions of long-running maint operations (Jason Dillaman)
* librbd: misc fixes (Xinxin Shu, Jason Dillaman)
* librbd: mock tests (Jason Dillaman)
* librbd: only export public API symbols (Jason Dillaman)
* librbd: optionally blacklist clients before breaking locks (#10761 Jason Dillaman)
* librbd: prevent copyup during shrink (Jason Dillaman)
* librbd: refactor unit tests to use fixtures (Jason Dillaman)
* librbd: validate image is r/w on resize/flatten (Jason Dillaman)
* librbd: various internal locking fixes (Jason Dillaman)
* many coverity fixes (Danny Al-Gaaf)
* many many coverity cleanups (Danny Al-Gaaf)
* mds: 'flush journal' admin command (John Spray)
* mds: ENOSPC and OSDMap epoch barriers (#7317 John Spray)
* mds: a whole bunch of initial scrub infrastructure (Greg Farnum)
* mds: add cephfs-table-tool (John Spray)
* mds: asok command for fetching subtree map (John Spray)
* mds: avoid sending traceless replies in most cases (Yan, Zheng)
* mds: constify MDSCacheObjects (John Spray)
* mds: dirfrag buf fix (Yan, Zheng)
* mds: disallow most commands on inactive MDS's (Greg Farnum)
* mds: drop dentries, leases on deleted directories (#10164 Yan, Zheng)
* mds: export dir asok command (John Spray)
* mds: fix MDLog IO callback deadlock (John Spray)
* mds: fix compat_version for MClientSession (#9945 John Spray)
* mds: fix deadlock during journal probe vs purge (#10229 Yan, Zheng)
* mds: fix race trimming log segments (Yan, Zheng)
* mds: fix reply snapbl (Yan, Zheng)
* mds: fix sessionmap lifecycle bugs (Yan, Zheng)
* mds: fix stray/purge perfcounters (#10388 John Spray)
* mds: handle heartbeat_reset during shutdown (#10382 John Spray)
* mds: handle zero-size xattr (#10335 Yan, Zheng)
* mds: initialize root inode xattr version (Yan, Zheng)
* mds: introduce auth caps (John Spray)
* mds: many many snapshot-related fixes (Yan, Zheng)
* mds: misc bugs (Greg Farnum, John Spray, Yan, Zheng, Henry Change)
* mds: refactor, improve Session storage (John Spray)
* mds: store backtrace for stray dir (Yan, Zheng)
* mds: subtree quota support (Yunchuan Wen)
* mds: verify backtrace when fetching dirfrag (#9557 Yan, Zheng)
* memstore: free space tracking (John Spray)
* misc cleanup (Danny Al-Gaaf, David Anderson)
* misc coverity fixes (Danny Al-Gaaf)
* misc coverity fixes (Danny Al-Gaaf)
* misc: various valgrind fixes and cleanups (Danny Al-Gaaf)
* mon: 'osd crush reweight-all' command (Sage Weil)
* mon: add 'ceph osd rename-bucket ...' command (Loic Dachary)
* mon: add bootstrap-rgw profile (Sage Weil)
* mon: add max pgs per osd warning (Sage Weil)
* mon: add noforward flag for some mon commands (Mykola Golub)
* mon: allow adding tiers to fs pools (#10135 John Spray)
* mon: allow full flag to be manually cleared (#9323 Sage Weil)
* mon: clean up auth list output (Loic Dachary)
* mon: delay failure injection (Joao Eduardo Luis)
* mon: disallow empty pool names (#10555 Wido den Hollander)
* mon: do not deactivate last mds (#10862 John Spray)
* mon: do not pollute mon dir with CSV files from CRUSH check (Loic Dachary)
* mon: drop old ceph_mon_store_converter (Sage Weil)
* mon: fix 'ceph pg dump_stuck degraded' (Xinxin Shu)
* mon: fix 'mds fail' for standby MDSs (John Spray)
* mon: fix 'osd crush link' id resolution (John Spray)
* mon: fix 'profile osd' use of config-key function on mon (#10844 Joao Eduardo Luis)
* mon: fix *_ratio* units and types (Sage Weil)
* mon: fix JSON dumps to dump floats as flots and not strings (Sage Weil)
* mon: fix MDS health status from peons (#10151 John Spray)
* mon: fix caching for min_last_epoch_clean (#9987 Sage Weil)
* mon: fix clock drift time check interval (#10546 Joao Eduardo Luis)
* mon: fix compatset initalization during mkfs (Joao Eduardo Luis)
* mon: fix error output for add_data_pool (#9852 Joao Eduardo Luis)
* mon: fix feature tracking during elections (Joao Eduardo Luis)
* mon: fix formatter 'pg stat' command output (Sage Weil)
* mon: fix mds gid/rank/state parsing (John Spray)
* mon: fix misc error paths (Joao Eduardo Luis)
* mon: fix paxos off-by-one corner case (#9301 Sage Weil)
* mon: fix paxos timeouts (#10220 Joao Eduardo Luis)
* mon: fix stashed monmap encoding (#5203 Xie Rui)
* mon: fix units in store stats (Joao Eduardo Luis)
* mon: get canonical OSDMap from leader (#10422 Sage Weil)
* mon: ignore failure reports from before up_from (#10762 Dan van der Ster, Sage Weil)
* mon: implement 'fs reset' command (John Spray)
* mon: improve error handling on erasure code profile set (#10488, #11144 Loic Dachary)
* mon: improved corrupt CRUSH map detection (Joao Eduardo Luis)
* mon: include entity name in audit log for forwarded requests (#9913 Joao Eduardo Luis)
* mon: include pg_temp count in osdmap summary (Sage Weil)
* mon: log health summary to cluster log (#9440 Joao Eduardo Luis)
* mon: make 'mds fail' idempotent (John Spray)
* mon: make pg dump {sum,pgs,pgs_brief} work for format=plain (#5963 #6759 Mykola Golub)
* mon: new 'ceph pool ls [detail]' command (Sage Weil)
* mon: new pool safety flags nodelete, nopgchange, nosizechange (#9792 Mykola Golub)
* mon: new, friendly 'ceph pg ls ...' command (Xinxin Shu)
* mon: paxos: allow reads while proposing (#9321 #9322 Joao Eduardo Luis)
* mon: prevent MDS transition from STOPPING (#10791 Greg Farnum)
* mon: propose all pending work in one transaction (Sage Weil)
* mon: remove pg_temps for nonexistent pools (Joao Eduardo Luis)
* mon: require mon_allow_pool_delete option to remove pools (Sage Weil)
* mon: respect down flag when promoting standbys (John Spray)
* mon: set globalid prealloc to larger value (Sage Weil)
* mon: set {read,write}_tier on 'osd tier add-cache ...' (Jianpeng Ma)
* mon: skip zeroed osd stats in get_rule_avail (#10257 Joao Eduardo Luis)
* mon: validate min_size range (Jianpeng Ma)
* mon: wait for writeable before cross-proposing (#9794 Joao Eduardo Luis)
* mount.ceph: fix suprious error message (#10351 Yan, Zheng)
* ms: xio: fix misc bugs (Matt Benjamin, Vu Pham)
* msgr: async: bind threads to CPU cores, improved poll (Haomai Wang)
* msgr: async: many fixes, unit tests (Haomai Wang)
* msgr: async: several fixes (Haomai Wang)
* msgr: asyncmessenger: add kqueue support (#9926 Haomai Wang)
* msgr: avoid useless new/delete (Haomai Wang)
* msgr: fix RESETSESSION bug (#10080 Greg Farnum)
* msgr: fix crc configuration (Mykola Golub)
* msgr: fix delay injection bug (#9910 Sage Weil, Greg Farnum)
* msgr: misc unit tests (Haomai Wang)
* msgr: new AsymcMessenger alternative implementation (Haomai Wang)
* msgr: prefetch data when doing recv (Yehuda Sadeh)
* msgr: simple: fix rare deadlock (Greg Farnum)
* msgr: simple: retry binding to port on failure (#10029 Wido den Hollander)
* msgr: xio: XioMessenger RDMA support (Casey Bodley, Vu Pham, Matt Benjamin)
* objectstore: deprecate collection attrs (Sage Weil)
* osd, librados: fadvise-style librados hints (Jianpeng Ma)
* osd, librados: fix xattr_cmp_u64 (Dongmao Zhang)
* osd, librados: revamp PG listing API to handle namespaces (#9031 #9262 #9438 David Zafman)
* osd, mds: 'ops' as shorthand for 'dump_ops_in_flight' on asok (Sage Weil)
* osd, mon: add checksums to all OSDMaps (Sage Weil)
* osd, mon: send intiial pg create time from mon to osd (#9887 David Zafman)
* osd,mon: add 'norebalance' flag (Kefu Chai)
* osd,mon: specify OSD features explicitly in MOSDBoot (#10911 Sage Weil)
* osd: DBObjectMap: fix locking to prevent rare crash (#9891 Samuel Just)
* osd: EIO on whole-object reads when checksum is wrong (Sage Weil)
* osd: add erasure code corpus (Loic Dachary)
* osd: add fadvise flags to ObjectStore API (Jianpeng Ma)
* osd: add get_latest_osdmap asok command (#9483 #9484 Mykola Golub)
* osd: add misc tests (Loic Dachary, Danny Al-Gaaf)
* osd: add option to prioritize heartbeat network traffic (Jian Wen)
* osd: add support for the SHEC erasure-code algorithm (Takeshi Miyamae, Loic Dachary)
* osd: allow deletion of objects with watcher (#2339 Sage Weil)
* osd: allow recovery while below min_size (Samuel Just)
* osd: allow recovery with fewer than min_size OSDs (Samuel Just)
* osd: allow sparse read for Push/Pull (Haomai Wang)
* osd: allow whiteout deletion in cache pool (Sage Weil)
* osd: allow writes to degraded objects (Samuel Just)
* osd: allow writes to degraded objects (Samuel Just)
* osd: avoid publishing unchanged PG stats (Sage Weil)
* osd: batch pg log trim (Xinze Chi)
* osd: cache pool: ignore min flush age when cache is full (Xinze Chi)
* osd: cache recent ObjectContexts (Dong Yuan)
* osd: cache reverse_nibbles hash value (Dong Yuan)
* osd: clean up internal ObjectStore interface (Sage Weil)
* osd: cleanup boost optionals (William Kennington)
* osd: clear cache on interval change (Samuel Just)
* osd: do no proxy reads unless target OSDs are new (#10788 Sage Weil)
* osd: do not abort deep scrub on missing hinfo (#10018 Loic Dachary)
* osd: do not update digest on inconsistent object (#10524 Samuel Just)
* osd: don't record digests for snapdirs (#10536 Samuel Just)
* osd: drop upgrade support for pre-dumpling (Sage Weil)
* osd: enable and use posix_fadvise (Sage Weil)
* osd: erasure coding: allow bench.sh to test ISA backend (Yuan Zhou)
* osd: erasure-code: encoding regression tests, corpus (#9420 Loic Dachary)
* osd: erasure-code: enforce chunk size alignment (#10211 Loic Dachary)
* osd: erasure-code: jerasure support for NEON (Loic Dachary)
* osd: erasure-code: relax cauchy w restrictions (#10325 David Zhang, Loic Dachary)
* osd: erasure-code: update gf-complete to latest upstream (Loic Dachary)
* osd: expose non-journal backends via ceph-osd CLI (Hoamai Wang)
* osd: filejournal: don't cache journal when not using direct IO (Jianpeng Ma)
* osd: fix JSON output for stray OSDs (Loic Dachary)
* osd: fix OSDCap parser on old (el6) boost::spirit (#10757 Kefu Chai)
* osd: fix OSDCap parsing on el6 (#10757 Kefu Chai)
* osd: fix ObjectStore::Transaction encoding version (#10734 Samuel Just)
* osd: fix WBTHrottle perf counters (Haomai Wang)
* osd: fix and document last_epoch_started semantics (Samuel Just)
* osd: fix auth object selection during repair (#10524 Samuel Just)
* osd: fix backfill bug (#10150 Samuel Just)
* osd: fix bug in pending digest updates (#10840 Samuel Just)
* osd: fix cancel_proxy_read_ops (Sage Weil)
* osd: fix cleanup of interrupted pg deletion (#10617 Sage Weil)
* osd: fix divergent entry handling on PG split (Samuel Just)
* osd: fix ghobject_t formatted output to include shard (#10063 Loic Dachary)
* osd: fix ioprio option (Mykola Golub)
* osd: fix ioprio options (Loic Dachary)
* osd: fix journal shutdown race (Sage Weil)
* osd: fix journal wrapping bug (#10883 David Zafman)
* osd: fix leak in SnapTrimWQ (#10421 Kefu Chai)
* osd: fix leak on shutdown (Kefu Chai)
* osd: fix memstore free space calculation (Xiaoxi Chen)
* osd: fix mixed-version peering issues (Samuel Just)
* osd: fix object age eviction (Zhiqiang Wang)
* osd: fix object atime calculation (Xinze Chi)
* osd: fix object digest update bug (#10840 Samuel Just)
* osd: fix occasional peering stalls (#10431 Sage Weil)
* osd: fix ordering issue with new transaction encoding (#10534 Dong Yuan)
* osd: fix osd peer check on scrub messages (#9555 Sage Weil)
* osd: fix past_interval display bug (#9752 Loic Dachary)
* osd: fix past_interval generation (#10427 #10430 David Zafman)
* osd: fix pgls filter ops (#9439 David Zafman)
* osd: fix recording of digest on scrub (Samuel Just)
* osd: fix scrub delay bug (#10693 Samuel Just)
* osd: fix scrub vs try-flush bug (#8011 Samuel Just)
* osd: fix short read handling on push (#8121 David Zafman)
* osd: fix stderr with -f or -d (Dan Mick)
* osd: fix transaction accounting (Jianpeng Ma)
* osd: fix watch reconnect race (#10441 Sage Weil)
* osd: fix watch timeout cache state update (#10784 David Zafman)
* osd: fix whiteout handling (Sage Weil)
* osd: flush snapshots from cache tier immediately (Sage Weil)
* osd: force promotion of watch/notify ops (Zhiqiang Wang)
* osd: handle no-op write with snapshot (#10262 Sage Weil)
* osd: improve idempotency detection across cache promotion/demotion (#8935 Sage Weil, Samuel Just)
* osd: include activating peers in blocked_by (#10477 Sage Weil)
* osd: jerasure and gf-complete updates from upstream (#10216 Loic Dachary)
* osd: journal: check fsync/fdatasync result (Jianpeng Ma)
* osd: journal: fix alignment checks, avoid useless memmove (Jianpeng Ma)
* osd: journal: fix hang on shutdown (#10474 David Zafman)
* osd: journal: fix header.committed_up_to (Xinze Chi)
* osd: journal: fix journal zeroing when direct IO is enabled (Xie Rui)
* osd: journal: initialize throttle (Ning Yao)
* osd: journal: misc bug fixes (#6003 David Zafman, Samuel Just)
* osd: journal: update committed_thru after replay (#6756 Samuel Just)
* osd: keyvaluestore: cleanup dead code (Ning Yao)
* osd: keyvaluestore: fix getattr semantics (Haomai Wang)
* osd: keyvaluestore: fix key ordering (#10119 Haomai Wang)
* osd: keyvaluestore_dev: optimization (Chendi Xue)
* osd: limit in-flight read requests (Jason Dillaman)
* osd: log when scrub or repair starts (Loic Dachary)
* osd: make misdirected op checks robust for EC pools (#9835 Sage Weil)
* osd: memstore: fix size limit (Xiaoxi Chen)
* osd: misc FIEMAP fixes (Ma Jianpeng)
* osd: misc cleanup (Xinze Chi, Yongyue Sun)
* osd: misc optimizations (Xinxin Shu, Zhiqiang Wang, Xinze Chi)
* osd: misc scrub fixes (#10017 Loic Dachary)
* osd: new 'activating' state between peering and active (Sage Weil)
* osd: new optimized encoding for ObjectStore::Transaction (Dong Yuan)
* osd: optimize Finisher (Xinze Chi)
* osd: optimize WBThrottle map with unordered_map (Ning Yao)
* osd: optimize filter_snapc (Ning Yao)
* osd: preserve reqids for idempotency checks for promote/demote (Sage Weil, Zhiqiang Wang, Samuel Just)
* osd: proxy read support (Zhiqiang Wang)
* osd: proxy reads during cache promote (Zhiqiang Wang)
* osd: remove dead locking code (Xinxin Shu)
* osd: remove legacy classic scrub code (Sage Weil)
* osd: remove unused fields in MOSDSubOp (Xiaoxi Chen)
* osd: removed some dead code (Xinze Chi)
* osd: replace MOSDSubOp messages with simpler, optimized MOSDRepOp (Xiaoxi Chen)
* osd: restrict scrub to certain times of day (Xinze Chi)
* osd: rocksdb: fix shutdown (Hoamai Wang)
* osd: store PG metadata in per-collection objects for better concurrency (Sage Weil)
* osd: store whole-object checksums on scrub, write_full (Sage Weil)
* osd: support for discard for journal trim (Jianpeng Ma)
* osd: use FIEMAP_FLAGS_SYNC instead of fsync (Jianpeng Ma)
* osd: verify kernel is new enough before using XFS extsize ioctl, enable by default (#9956 Sage Weil)
* pybind: fix memory leak in librados bindings (Billy Olsen)
* pyrados: add object lock support (#6114 Mehdi Abaakouk)
* pyrados: fix misnamed wait_* routings (#10104 Dan Mick)
* pyrados: misc cleanups (Kefu Chai)
* qa: add large auth ticket tests (Ilya Dryomov)
* qa: fix mds tests (#10539 John Spray)
* qa: fix osd create dup tests (#10083 Loic Dachary)
* qa: ignore duplicates in rados ls (Josh Durgin)
* qa: improve hadoop tests (Noah Watkins)
* qa: many 'make check' improvements (Loic Dachary)
* qa: misc tests (Loic Dachary, Yan, Zheng)
* qa: parallelize make check (Loic Dachary)
* qa: reorg fs quota tests (Greg Farnum)
* qa: tolerate nearly-full disk for make check (Loic Dachary)
* rados: fix put of /dev/null (Loic Dachary)
* rados: fix usage (Jianpeng Ma)
* rados: parse command-line arguments more strictly (#8983 Adam Crume)
* rados: use copy-from operation for copy, cppool (Sage Weil)
* radosgw-admin: add replicalog update command (Yehuda Sadeh)
* rbd-fuse: clean up on shutdown (Josh Durgin)
* rbd-fuse: fix memory leak (Adam Crume)
* rbd-replay-many (Adam Crume)
* rbd-replay: --anonymize flag to rbd-replay-prep (Adam Crume)
* rbd: add 'merge-diff' function (MingXin Liu, Yunchuan Wen, Li Wang)
* rbd: allow v2 striping parameters for clones and imports (Jason Dillaman)
* rbd: fix 'rbd diff' for non-existent objects (Adam Crume)
* rbd: fix buffer handling on image import (#10590 Jason Dillaman)
* rbd: fix error when striping with format 1 (Sebastien Han)
* rbd: fix export for image sizes over 2GB (Vicente Cheng)
* rbd: fix formatted output of image features (Jason Dillaman)
* rbd: leave exclusive lockin goff by default (Jason Dillaman)
* rbd: updat eman page (Ilya Dryomov)
* rbd: update init-rbdmap to fix dup mount point (Karel Striegel)
* rbd: use IO hints for import, export, and bench operations (#10462 Jason Dillaman)
* rbd: use rolling average for rbd bench-write throughput (Jason Dillaman)
* rbd_recover_tool: RBD image recovery tool (Min Chen)
* rgw: S3-style object versioning support (Yehuda Sadeh)
* rgw: add location header when object is in another region (VRan Liu)
* rgw: change multipart upload id magic (#10271 Yehuda Sadeh)
* rgw: check keystone auth for S3 POST requests (#10062 Abhishek Lekshmanan)
* rgw: check timestamp on s3 keystone auth (#10062 Abhishek Lekshmanan)
* rgw: conditional PUT on ETag (#8562 Ray Lv)
* rgw: create subuser if needed when creating user (#10103 Yehuda Sadeh)
* rgw: decode http query params correction (#10271 Yehuda Sadeh)
* rgw: don't overwrite bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: enable IPv6 for civetweb (#10965 Yehuda Sadeh)
* rgw: extend replica log API (purge-all) (Yehuda Sadeh)
* rgw: fail S3 POST if keystone not configured (#10688 Valery Tschopp, Yehuda Sadeh)
* rgw: fix If-Modified-Since (VRan Liu)
* rgw: fix XML header on get ACL request (#10106 Yehuda Sadeh)
* rgw: fix bucket removal with data purge (Yehuda Sadeh)
* rgw: fix content length check (#10701 Axel Dunkel, Yehuda Sadeh)
* rgw: fix content-length update (#9576 Yehuda Sadeh)
* rgw: fix disabling of max_size quota (#9907 Dong Lei)
* rgw: fix error codes (#10334 #10329 Yehuda Sadeh)
* rgw: fix incorrect len when len is 0 (#9877 Yehuda Sadeh)
* rgw: fix object copy content type (#9478 Yehuda Sadeh)
* rgw: fix partial GET in swift (#10553 Yehuda Sadeh)
* rgw: fix replica log indexing (#8251 Yehuda Sadeh)
* rgw: fix shutdown (#10472 Yehuda Sadeh)
* rgw: fix swift metadata header name (Dmytro Iurchenko)
* rgw: fix sysvinit script when rgw_socket_path is not defined (#11159 Yehuda Sadeh, Dan Mick)
* rgw: fix user stags in get-user-info API (#9359 Ray Lv)
* rgw: include XML ns on get ACL request (#10106 Yehuda Sadeh)
* rgw: index swift keys appropriately (#10471 Yehuda Sadeh)
* rgw: make sysvinit script set ulimit -n properly (Sage Weil)
* rgw: misc fixes (#10307 Yehuda Sadeh)
* rgw: only track cleanup for objects we write (#10311 Yehuda Sadeh)
* rgw: pass civetweb configurables through (#10907 Yehuda Sadeh)
* rgw: prevent illegal bucket policy that doesn't match placement rule (Yehuda Sadeh)
* rgw: remove multipart entries from bucket index on abort (#10719 Yehuda Sadeh)
* rgw: remove swift user manifest (DLO) hash calculation (#9973 Yehuda Sadeh)
* rgw: respond with 204 to POST on containers (#10667 Yuan Zhou)
* rgw: return timestamp on GET/HEAD (#8911 Yehuda Sadeh)
* rgw: reuse fcgx connection struct (#10194 Yehuda Sadeh)
* rgw: run radosgw as apache with systemd (#10125 Loic Dachary)
* rgw: send explicit HTTP status string (Yehuda Sadeh)
* rgw: set ETag on object copy (#9479 Yehuda Sadeh)
* rgw: set length for keystone token validation request (#7796 Yehuda Sadeh, Mark Kirkwood)
* rgw: support X-Storage-Policy header for Swift storage policy compat (Yehuda Sadeh)
* rgw: support multiple host names (#7467 Yehuda Sadeh)
* rgw: swift: dump container's custom metadata (#10665 Ahmad Faheem, Dmytro Iurchenko)
* rgw: swift: support Accept header for response format (#10746 Dmytro Iurchenko)
* rgw: swift: support for X-Remove-Container-Meta-{key} (#10475 Dmytro Iurchenko)
* rgw: tweak error codes (#10329 #10334 Yehuda Sadeh)
* rgw: update bucket index on attr changes, for multi-site sync (#5595 Yehuda Sadeh)
* rgw: use \r\n for http headers (#9254 Yehuda Sadeh)
* rgw: use gc for multipart abort (#10445 Aaron Bassett, Yehuda Sadeh)
* rgw: use new watch/notify API (Yehuda Sadeh, Sage Weil)
* rpm: misc fixes (Key Dreyer)
* rpm: move rgw logrotate to radosgw subpackage (Ken Dreyer)
* systemd: better systemd unit files (Owen Synge)
* sysvinit: fix race in 'stop' (#10389 Loic Dachary)
* test: fix bufferlist tests (Jianpeng Ma)
* tests: ability to run unit tests under docker (Loic Dachary)
* tests: centos-6 dockerfile (#10755 Loic Dachary)
* tests: improve docker-based tests (Loic Dachary)
* tests: unit tests for shared_cache (Dong Yuan)
* udev: fix rules for CentOS7/RHEL7 (Loic Dachary)
* use clock_gettime instead of gettimeofday (Jianpeng Ma)
* vstart.sh: set up environment for s3-tests (Luis Pabon)
* vstart.sh: work with cmake (Yehuda Sadeh)






v0.93
=====

This is the first release candidate for Hammer, and includes all of
the features that will be present in the final release.  We welcome
and encourage any and all testing in non-production clusters to identify
any problems with functionality, stability, or performance before the
final Hammer release.

We suggest some caution in one area: librbd.  There is a lot of new
functionality around object maps and locking that is disabled by
default but may still affect stability for existing images.  We are
continuing to shake out those bugs so that the final Hammer release
(probably v0.94) will be rock solid.

Major features since Giant include:

* cephfs: journal scavenger repair tool (John Spray)
* crush: new and improved straw2 bucket type (Sage Weil, Christina Anderson, Xiaoxi Chen)
* doc: improved guidance for CephFS early adopters (John Spray)
* librbd: add per-image object map for improved performance (Jason Dillaman)
* librbd: copy-on-read (Min Chen, Li Wang, Yunchuan Wen, Cheng Cheng)
* librados: fadvise-style IO hints (Jianpeng Ma)
* mds: many many snapshot-related fixes (Yan, Zheng)
* mon: new 'ceph osd df' command (Mykola Golub)
* mon: new 'ceph pg ls ...' command (Xinxin Shu)
* osd: improved performance for high-performance backends
* osd: improved recovery behavior (Samuel Just)
* osd: improved cache tier behavior with reads (Zhiqiang Wang)
* rgw: S3-compatible bucket versioning support (Yehuda Sadeh)
* rgw: large bucket index sharding (Guang Yang, Yehuda Sadeh)
* RDMA "xio" messenger support (Matt Benjamin, Vu Pham)

Upgrading
---------

* If you are upgrading from v0.92, you must stop all OSD daemons and flush their
  journals (``ceph-osd -i NNN --flush-journal``) before upgrading.  There was
  a transaction encoding bug in v0.92 that broke compatibility.  Upgrading from
  v0.91 or anything earlier is safe.

* No special restrictions when upgrading from firefly or giant.

Notable Changes
---------------

* build: CMake support (Ali Maredia, Casey Bodley, Adam Emerson, Marcus Watts, Matt Benjamin)
* ceph-disk: do not re-use partition if encryption is required (Loic Dachary)
* ceph-disk: support LUKS for encrypted partitions (Andrew Bartlett, Loic Dachary)
* ceph-fuse,libcephfs: add support for O_NOFOLLOW and O_PATH (Greg Farnum)
* ceph-fuse,libcephfs: resend requests before completing cap reconnect (#10912 Yan, Zheng)
* ceph-fuse: select kernel cache invalidation mechanism based on kernel version (Greg Farnum)
* ceph-objectstore-tool: improved import (David Zafman)
* ceph-objectstore-tool: misc improvements, fixes (#9870 #9871 David Zafman)
* ceph: add 'ceph osd df [tree]' command (#10452 Mykola Golub)
* ceph: fix 'ceph tell ...' command validation (#10439 Joao Eduardo Luis)
* ceph: improve 'ceph osd tree' output (Mykola Golub)
* cephfs-journal-tool: add recover_dentries function (#9883 John Spray)
* common: add newline to flushed json output (Sage Weil)
* common: filtering for 'perf dump' (John Spray)
* common: fix Formatter factory breakage (#10547 Loic Dachary)
* common: make json-pretty output prettier (Sage Weil)
* crush: new and improved straw2 bucket type (Sage Weil, Christina Anderson, Xiaoxi Chen)
* crush: update tries stats for indep rules (#10349 Loic Dachary)
* crush: use larger choose_tries value for erasure code rulesets (#10353 Loic Dachary)
* debian,rpm: move RBD udev rules to ceph-common (#10864 Ken Dreyer)
* debian: split python-ceph into python-{rbd,rados,cephfs} (Boris Ranto)
* doc: CephFS disaster recovery guidance (John Spray)
* doc: CephFS for early adopters (John Spray)
* doc: fix OpenStack Glance docs (#10478 Sebastien Han)
* doc: misc updates (#9793 #9922 #10204 #10203 Travis Rhoden, Hazem, Ayari, Florian Coste, Andy Allan, Frank Yu, Baptiste Veuillez-Mainard, Yuan Zhou, Armando Segnini, Robert Jansen, Tyler Brekke, Viktor Suprun)
* doc: replace cloudfiles with swiftclient Python Swift example (Tim Freund)
* erasure-code: add mSHEC erasure code support (Takeshi Miyamae)
* erasure-code: improved docs (#10340 Loic Dachary)
* erasure-code: set max_size to 20 (#10363 Loic Dachary)
* libcephfs,ceph-fuse: fix getting zero-length xattr (#10552 Yan, Zheng)
* librados: add blacklist_add convenience method (Jason Dillaman)
* librados: expose rados_{read|write}_op_assert_version in C API (Kim Vandry)
* librados: fix pool name caching (#10458 Radoslaw Zarzynski)
* librados: fix resource leak, misc bugs (#10425 Radoslaw Zarzynski)
* librados: fix some watch/notify locking (Jason Dillaman, Josh Durgin)
* libradosstriper: fix write_full when ENOENT (#10758 Sebastien Ponce)
* librbd: CRC protection for RBD image map (Jason Dillaman)
* librbd: add per-image object map for improved performance (Jason Dillaman)
* librbd: add support for an "object map" indicating which objects exist (Jason Dillaman)
* librbd: adjust internal locking (Josh Durgin, Jason Dillaman)
* librbd: better handling of watch errors (Jason Dillaman)
* librbd: coordinate maint operations through lock owner (Jason Dillaman)
* librbd: copy-on-read (Min Chen, Li Wang, Yunchuan Wen, Cheng Cheng, Jason Dillaman)
* librbd: enforce write ordering with a snapshot (Jason Dillaman)
* librbd: fadvise-style hints; add misc hints for certain operations (Jianpeng Ma)
* librbd: fix coverity false-positives (Jason Dillaman)
* librbd: fix snap create races (Jason Dillaman)
* librbd: flush AIO operations asynchronously (#10714 Jason Dillaman)
* librbd: make async versions of long-running maint operations (Jason Dillaman)
* librbd: mock tests (Jason Dillaman)
* librbd: optionally blacklist clients before breaking locks (#10761 Jason Dillaman)
* librbd: prevent copyup during shrink (Jason Dillaman)
* mds: add cephfs-table-tool (John Spray)
* mds: avoid sending traceless replies in most cases (Yan, Zheng)
* mds: export dir asok command (John Spray)
* mds: fix stray/purge perfcounters (#10388 John Spray)
* mds: handle heartbeat_reset during shutdown (#10382 John Spray)
* mds: many many snapshot-related fixes (Yan, Zheng)
* mds: refactor, improve Session storage (John Spray)
* misc coverity fixes (Danny Al-Gaaf)
* mon: add noforward flag for some mon commands (Mykola Golub)
* mon: disallow empty pool names (#10555 Wido den Hollander)
* mon: do not deactivate last mds (#10862 John Spray)
* mon: drop old ceph_mon_store_converter (Sage Weil)
* mon: fix 'ceph pg dump_stuck degraded' (Xinxin Shu)
* mon: fix 'profile osd' use of config-key function on mon (#10844 Joao Eduardo Luis)
* mon: fix compatset initalization during mkfs (Joao Eduardo Luis)
* mon: fix feature tracking during elections (Joao Eduardo Luis)
* mon: fix mds gid/rank/state parsing (John Spray)
* mon: ignore failure reports from before up_from (#10762 Dan van der Ster, Sage Weil)
* mon: improved corrupt CRUSH map detection (Joao Eduardo Luis)
* mon: include pg_temp count in osdmap summary (Sage Weil)
* mon: log health summary to cluster log (#9440 Joao Eduardo Luis)
* mon: make 'mds fail' idempotent (John Spray)
* mon: make pg dump {sum,pgs,pgs_brief} work for format=plain (#5963 #6759 Mykola Golub)
* mon: new pool safety flags nodelete, nopgchange, nosizechange (#9792 Mykola Golub)
* mon: new, friendly 'ceph pg ls ...' command (Xinxin Shu)
* mon: prevent MDS transition from STOPPING (#10791 Greg Farnum)
* mon: propose all pending work in one transaction (Sage Weil)
* mon: remove pg_temps for nonexistent pools (Joao Eduardo Luis)
* mon: require mon_allow_pool_delete option to remove pools (Sage Weil)
* mon: set globalid prealloc to larger value (Sage Weil)
* mon: skip zeroed osd stats in get_rule_avail (#10257 Joao Eduardo Luis)
* mon: validate min_size range (Jianpeng Ma)
* msgr: async: bind threads to CPU cores, improved poll (Haomai Wang)
* msgr: fix crc configuration (Mykola Golub)
* msgr: misc unit tests (Haomai Wang)
* msgr: xio: XioMessenger RDMA support (Casey Bodley, Vu Pham, Matt Benjamin)
* osd, librados: fadvise-style librados hints (Jianpeng Ma)
* osd, librados: fix xattr_cmp_u64 (Dongmao Zhang)
* osd,mon: add 'norebalance' flag (Kefu Chai)
* osd,mon: specify OSD features explicitly in MOSDBoot (#10911 Sage Weil)
* osd: add option to prioritize heartbeat network traffic (Jian Wen)
* osd: add support for the SHEC erasure-code algorithm (Takeshi Miyamae, Loic Dachary)
* osd: allow recovery while below min_size (Samuel Just)
* osd: allow recovery with fewer than min_size OSDs (Samuel Just)
* osd: allow writes to degraded objects (Samuel Just)
* osd: allow writes to degraded objects (Samuel Just)
* osd: avoid publishing unchanged PG stats (Sage Weil)
* osd: cache recent ObjectContexts (Dong Yuan)
* osd: clear cache on interval change (Samuel Just)
* osd: do no proxy reads unless target OSDs are new (#10788 Sage Weil)
* osd: do not update digest on inconsistent object (#10524 Samuel Just)
* osd: don't record digests for snapdirs (#10536 Samuel Just)
* osd: fix OSDCap parser on old (el6) boost::spirit (#10757 Kefu Chai)
* osd: fix OSDCap parsing on el6 (#10757 Kefu Chai)
* osd: fix ObjectStore::Transaction encoding version (#10734 Samuel Just)
* osd: fix auth object selection during repair (#10524 Samuel Just)
* osd: fix bug in pending digest updates (#10840 Samuel Just)
* osd: fix cancel_proxy_read_ops (Sage Weil)
* osd: fix cleanup of interrupted pg deletion (#10617 Sage Weil)
* osd: fix journal wrapping bug (#10883 David Zafman)
* osd: fix leak in SnapTrimWQ (#10421 Kefu Chai)
* osd: fix memstore free space calculation (Xiaoxi Chen)
* osd: fix mixed-version peering issues (Samuel Just)
* osd: fix object digest update bug (#10840 Samuel Just)
* osd: fix ordering issue with new transaction encoding (#10534 Dong Yuan)
* osd: fix past_interval generation (#10427 #10430 David Zafman)
* osd: fix short read handling on push (#8121 David Zafman)
* osd: fix watch timeout cache state update (#10784 David Zafman)
* osd: force promotion of watch/notify ops (Zhiqiang Wang)
* osd: improve idempotency detection across cache promotion/demotion (#8935 Sage Weil, Samuel Just)
* osd: include activating peers in blocked_by (#10477 Sage Weil)
* osd: jerasure and gf-complete updates from upstream (#10216 Loic Dachary)
* osd: journal: check fsync/fdatasync result (Jianpeng Ma)
* osd: journal: fix hang on shutdown (#10474 David Zafman)
* osd: journal: fix header.committed_up_to (Xinze Chi)
* osd: journal: initialize throttle (Ning Yao)
* osd: journal: misc bug fixes (#6003 David Zafman, Samuel Just)
* osd: misc cleanup (Xinze Chi, Yongyue Sun)
* osd: new 'activating' state between peering and active (Sage Weil)
* osd: preserve reqids for idempotency checks for promote/demote (Sage Weil, Zhiqiang Wang, Samuel Just)
* osd: remove dead locking code (Xinxin Shu)
* osd: restrict scrub to certain times of day (Xinze Chi)
* osd: rocksdb: fix shutdown (Hoamai Wang)
* pybind: fix memory leak in librados bindings (Billy Olsen)
* qa: fix mds tests (#10539 John Spray)
* qa: ignore duplicates in rados ls (Josh Durgin)
* qa: improve hadoop tests (Noah Watkins)
* qa: reorg fs quota tests (Greg Farnum)
* rados: fix usage (Jianpeng Ma)
* radosgw-admin: add replicalog update command (Yehuda Sadeh)
* rbd-fuse: clean up on shutdown (Josh Durgin)
* rbd: add 'merge-diff' function (MingXin Liu, Yunchuan Wen, Li Wang)
* rbd: fix buffer handling on image import (#10590 Jason Dillaman)
* rbd: leave exclusive lockin goff by default (Jason Dillaman)
* rbd: update init-rbdmap to fix dup mount point (Karel Striegel)
* rbd: use IO hints for import, export, and bench operations (#10462 Jason Dillaman)
* rbd_recover_tool: RBD image recovery tool (Min Chen)
* rgw: S3-style object versioning support (Yehuda Sadeh)
* rgw: check keystone auth for S3 POST requests (#10062 Abhishek Lekshmanan)
* rgw: extend replica log API (purge-all) (Yehuda Sadeh)
* rgw: fail S3 POST if keystone not configured (#10688 Valery Tschopp, Yehuda Sadeh)
* rgw: fix XML header on get ACL request (#10106 Yehuda Sadeh)
* rgw: fix bucket removal with data purge (Yehuda Sadeh)
* rgw: fix replica log indexing (#8251 Yehuda Sadeh)
* rgw: fix swift metadata header name (Dmytro Iurchenko)
* rgw: remove multipart entries from bucket index on abort (#10719 Yehuda Sadeh)
* rgw: respond with 204 to POST on containers (#10667 Yuan Zhou)
* rgw: reuse fcgx connection struct (#10194 Yehuda Sadeh)
* rgw: support multiple host names (#7467 Yehuda Sadeh)
* rgw: swift: dump container's custom metadata (#10665 Ahmad Faheem, Dmytro Iurchenko)
* rgw: swift: support Accept header for response format (#10746 Dmytro Iurchenko)
* rgw: swift: support for X-Remove-Container-Meta-{key} (#10475 Dmytro Iurchenko)
* rpm: move rgw logrotate to radosgw subpackage (Ken Dreyer)
* tests: centos-6 dockerfile (#10755 Loic Dachary)
* tests: unit tests for shared_cache (Dong Yuan)
* vstart.sh: work with cmake (Yehuda Sadeh)



v0.92
=====

This is the second-to-last chunk of new stuff before Hammer.  Big items
include additional checksums on OSD objects, proxied reads in the
cache tier, image locking in RBD, optimized OSD Transaction and
replication messages, and a big pile of RGW and MDS bug fixes.

Upgrading
---------

* The experimental 'keyvaluestore-dev' OSD backend has been renamed
  'keyvaluestore' (for simplicity) and marked as experimental.  To
  enable this untested feature and acknowledge that you understand
  that it is untested and may destroy data, you need to add the
  following to your ceph.conf::

    enable experimental unrecoverable data corrupting featuers = keyvaluestore

* The following librados C API function calls take a 'flags' argument whose value
  is now correctly interpreted:

     rados_write_op_operate()
     rados_aio_write_op_operate()
     rados_read_op_operate()
     rados_aio_read_op_operate()

  The flags were not correctly being translated from the librados constants to the
  internal values.  Now they are.  Any code that is passing flags to these methods
  should be audited to ensure that they are using the correct LIBRADOS_OP_FLAG_*
  constants.

* The 'rados' CLI 'copy' and 'cppool' commands now use the copy-from operation,
  which means the latest CLI cannot run these commands against pre-firefly OSDs.

* The librados watch/notify API now includes a watch_flush() operation to flush
  the async queue of notify operations.  This should be called by any watch/notify
  user prior to rados_shutdown().

Notable Changes
---------------

* add experimental features option (Sage Weil)
* build: fix 'make check' races (#10384 Loic Dachary)
* build: fix pkg names when libkeyutils is missing (Pankag Garg, Ken Dreyer)
* ceph: make 'ceph -s' show PG state counts in sorted order (Sage Weil)
* ceph: make 'ceph tell mon.* version' work (Mykola Golub)
* ceph-monstore-tool: fix/improve CLI (Joao Eduardo Luis)
* ceph: show primary-affinity in 'ceph osd tree' (Mykola Golub)
* common: add TableFormatter (Andreas Peters)
* common: check syncfs() return code (Jianpeng Ma)
* doc: do not suggest dangerous XFS nobarrier option (Dan van der Ster)
* doc: misc updates (Nilamdyuti Goswami, John Wilkins)
* install-deps.sh: do not require sudo when root (Loic Dachary)
* libcephfs: fix dirfrag trimming (#10387 Yan, Zheng)
* libcephfs: fix mount timeout (#10041 Yan, Zheng)
* libcephfs: fix test (#10415 Yan, Zheng)
* libcephfs: fix use-afer-free on umount (#10412 Yan, Zheng)
* libcephfs: include ceph and git version in client metadata (Sage Weil)
* librados: add watch_flush() operation (Sage Weil, Haomai Wang)
* librados: avoid memcpy on getxattr, read (Jianpeng Ma)
* librados: create ioctx by pool id (Jason Dillaman)
* librados: do notify completion in fast-dispatch (Sage Weil)
* librados: remove shadowed variable (Kefu Chain)
* librados: translate op flags from C APIs (Matthew Richards)
* librbd: differentiate between R/O vs R/W features (Jason Dillaman)
* librbd: exclusive image locking (Jason Dillaman)
* librbd: fix write vs import race (#10590 Jason Dillaman)
* librbd: gracefully handle deleted/renamed pools (#10270 Jason Dillaman)
* mds: asok command for fetching subtree map (John Spray)
* mds: constify MDSCacheObjects (John Spray)
* misc: various valgrind fixes and cleanups (Danny Al-Gaaf)
* mon: fix 'mds fail' for standby MDSs (John Spray)
* mon: fix stashed monmap encoding (#5203 Xie Rui)
* mon: implement 'fs reset' command (John Spray)
* mon: respect down flag when promoting standbys (John Spray)
* mount.ceph: fix suprious error message (#10351 Yan, Zheng)
* msgr: async: many fixes, unit tests (Haomai Wang)
* msgr: simple: retry binding to port on failure (#10029 Wido den Hollander)
* osd: add fadvise flags to ObjectStore API (Jianpeng Ma)
* osd: add get_latest_osdmap asok command (#9483 #9484 Mykola Golub)
* osd: EIO on whole-object reads when checksum is wrong (Sage Weil)
* osd: filejournal: don't cache journal when not using direct IO (Jianpeng Ma)
* osd: fix ioprio option (Mykola Golub)
* osd: fix scrub delay bug (#10693 Samuel Just)
* osd: fix watch reconnect race (#10441 Sage Weil)
* osd: handle no-op write with snapshot (#10262 Sage Weil)
* osd: journal: fix journal zeroing when direct IO is enabled (Xie Rui)
* osd: keyvaluestore: cleanup dead code (Ning Yao)
* osd, mds: 'ops' as shorthand for 'dump_ops_in_flight' on asok (Sage Weil)
* osd: memstore: fix size limit (Xiaoxi Chen)
* osd: misc scrub fixes (#10017 Loic Dachary)
* osd: new optimized encoding for ObjectStore::Transaction (Dong Yuan)
* osd: optimize filter_snapc (Ning Yao)
* osd: optimize WBThrottle map with unordered_map (Ning Yao)
* osd: proxy reads during cache promote (Zhiqiang Wang)
* osd: proxy read support (Zhiqiang Wang)
* osd: remove legacy classic scrub code (Sage Weil)
* osd: remove unused fields in MOSDSubOp (Xiaoxi Chen)
* osd: replace MOSDSubOp messages with simpler, optimized MOSDRepOp (Xiaoxi Chen)
* osd: store whole-object checksums on scrub, write_full (Sage Weil)
* osd: verify kernel is new enough before using XFS extsize ioctl, enable by default (#9956 Sage Weil)
* rados: use copy-from operation for copy, cppool (Sage Weil)
* rgw: change multipart upload id magic (#10271 Yehuda Sadeh)
* rgw: decode http query params correction (#10271 Yehuda Sadeh)
* rgw: fix content length check (#10701 Axel Dunkel, Yehuda Sadeh)
* rgw: fix partial GET in swift (#10553 Yehuda Sadeh)
* rgw: fix shutdown (#10472 Yehuda Sadeh)
* rgw: include XML ns on get ACL request (#10106 Yehuda Sadeh)
* rgw: misc fixes (#10307 Yehuda Sadeh)
* rgw: only track cleanup for objects we write (#10311 Yehuda Sadeh)
* rgw: tweak error codes (#10329 #10334 Yehuda Sadeh)
* rgw: use gc for multipart abort (#10445 Aaron Bassett, Yehuda Sadeh)
* sysvinit: fix race in 'stop' (#10389 Loic Dachary)
* test: fix bufferlist tests (Jianpeng Ma)
* tests: improve docker-based tests (Loic Dachary)


v0.91
=====

We are quickly approaching the Hammer feature freeze but have a few
more dev releases to go before we get there.  The headline items are
subtree-based quota support in CephFS (ceph-fuse/libcephfs client
support only for now), a rewrite of the watch/notify librados API used
by RBD and RGW, OSDMap checksums to ensure that maps are always
consistent inside the cluster, new API calls in librados and librbd
for IO hinting modeled after posix_fadvise, and improved storage of
per-PG state.

We expect two more releases before the Hammer feature freeze (v0.93).

Upgrading
---------

* The 'category' field for objects has been removed.  This was originally added
  to track PG stat summations over different categories of objects for use by
  radosgw.  It is no longer has any known users and is prone to abuse because it
  can lead to a pg_stat_t structure that is unbounded.  The librados API calls
  that accept this field now ignore it, and the OSD no longers tracks the
  per-category summations.

* The output for 'rados df' has changed.  The 'category' level has been
  eliminated, so there is now a single stat object per pool.  The structure of
  the JSON output is different, and the plaintext output has one less column.

* The 'rados create <objectname> [category]' optional category argument is no
  longer supported or recognized.

* rados.py's Rados class no longer has a __del__ method; it was causing
  problems on interpreter shutdown and use of threads.  If your code has
  Rados objects with limited lifetimes and you're concerned about locked
  resources, call Rados.shutdown() explicitly.

* There is a new version of the librados watch/notify API with vastly
  improved semantics.  Any applications using this interface are
  encouraged to migrate to the new API.  The old API calls are marked
  as deprecated and will eventually be removed.

* The librados rados_unwatch() call used to be safe to call on an
  invalid handle.  The new version has undefined behavior when passed
  a bogus value (for example, when rados_watch() returns an error and
  handle is not defined).

* The structure of the formatted 'pg stat' command is changed for the
  portion that counts states by name to avoid using the '+' character
  (which appears in state names) as part of the XML token (it is not
  legal).

Notable Changes
---------------

* asyncmsgr: misc fixes (Haomai Wang)
* buffer: add 'shareable' construct (Matt Benjamin)
* build: aarch64 build fixes (Noah Watkins, Haomai Wang)
* build: support for jemalloc (Shishir Gowda)
* ceph-disk: allow journal partition re-use (#10146 Loic Dachary, Dav van der Ster)
* ceph-disk: misc fixes (Christos Stavrakakis)
* ceph-fuse: fix kernel cache trimming (#10277 Yan, Zheng)
* ceph-objectstore-tool: many many improvements (David Zafman)
* common: support new gperftools header locations (Key Dreyer)
* crush: straw bucket weight calculation fixes (#9998 Sage Weil)
* doc: misc improvements (Nilamdyuti Goswami, John Wilkins, Chris Holcombe)
* libcephfs,ceph-fuse: add 'status' asok (John Spray)
* librados, osd: new watch/notify implementation (Sage Weil)
* librados: drop 'category' feature (Sage Weil)
* librados: fix pool deletion handling (#10372 Sage Weil)
* librados: new fadvise API (Ma Jianpeng)
* libradosstriper: fix remove() (Dongmao Zhang)
* librbd: complete pending ops before closing image (#10299 Josh Durgin)
* librbd: fadvise API (Ma Jianpeng)
* mds: ENOSPC and OSDMap epoch barriers (#7317 John Spray)
* mds: dirfrag buf fix (Yan, Zheng)
* mds: disallow most commands on inactive MDS's (Greg Farnum)
* mds: drop dentries, leases on deleted directories (#10164 Yan, Zheng)
* mds: handle zero-size xattr (#10335 Yan, Zheng)
* mds: subtree quota support (Yunchuan Wen)
* memstore: free space tracking (John Spray)
* misc cleanup (Danny Al-Gaaf, David Anderson)
* mon: 'osd crush reweight-all' command (Sage Weil)
* mon: allow full flag to be manually cleared (#9323 Sage Weil)
* mon: delay failure injection (Joao Eduardo Luis)
* mon: fix paxos timeouts (#10220 Joao Eduardo Luis)
* mon: get canonical OSDMap from leader (#10422 Sage Weil)
* msgr: fix RESETSESSION bug (#10080 Greg Farnum)
* objectstore: deprecate collection attrs (Sage Weil)
* osd, mon: add checksums to all OSDMaps (Sage Weil)
* osd: allow deletion of objects with watcher (#2339 Sage Weil)
* osd: allow sparse read for Push/Pull (Haomai Wang)
* osd: cache reverse_nibbles hash value (Dong Yuan)
* osd: drop upgrade support for pre-dumpling (Sage Weil)
* osd: enable and use posix_fadvise (Sage Weil)
* osd: erasure-code: enforce chunk size alignment (#10211 Loic Dachary)
* osd: erasure-code: jerasure support for NEON (Loic Dachary)
* osd: erasure-code: relax cauchy w restrictions (#10325 David Zhang, Loic Dachary)
* osd: erasure-code: update gf-complete to latest upstream (Loic Dachary)
* osd: fix WBTHrottle perf counters (Haomai Wang)
* osd: fix backfill bug (#10150 Samuel Just)
* osd: fix occasional peering stalls (#10431 Sage Weil)
* osd: fix scrub vs try-flush bug (#8011 Samuel Just)
* osd: fix stderr with -f or -d (Dan Mick)
* osd: misc FIEMAP fixes (Ma Jianpeng)
* osd: optimize Finisher (Xinze Chi)
* osd: store PG metadata in per-collection objects for better concurrency (Sage Weil)
* pyrados: add object lock support (#6114 Mehdi Abaakouk)
* pyrados: fix misnamed wait_* routings (#10104 Dan Mick)
* pyrados: misc cleanups (Kefu Chai)
* qa: add large auth ticket tests (Ilya Dryomov)
* qa: many 'make check' improvements (Loic Dachary)
* qa: misc tests (Loic Dachary, Yan, Zheng)
* rgw: conditional PUT on ETag (#8562 Ray Lv)
* rgw: fix error codes (#10334 #10329 Yehuda Sadeh)
* rgw: index swift keys appropriately (#10471 Yehuda Sadeh)
* rgw: prevent illegal bucket policy that doesn't match placement rule (Yehuda Sadeh)
* rgw: run radosgw as apache with systemd (#10125 Loic Dachary)
* rgw: support X-Storage-Policy header for Swift storage policy compat (Yehuda Sadeh)
* rgw: use \r\n for http headers (#9254 Yehuda Sadeh)
* rpm: misc fixes (Key Dreyer)


v0.90
=====

This is the last development release before Christmas.  There are some
API cleanups for librados and librbd, and lots of bug fixes across the
board for the OSD, MDS, RGW, and CRUSH.  The OSD also gets support for
discard (potentially helpful on SSDs, although it is off by default), and there
are several improvements to ceph-disk.

The next two development releases will be getting a slew of new
functionality for hammer.  Stay tuned!

Upgrading
---------

* Previously, the formatted output of 'ceph pg stat -f ...' was a full
  pg dump that included all metadata about all PGs in the system.  It
  is now a concise summary of high-level PG stats, just like the
  unformatted 'ceph pg stat' command.

* All JSON dumps of floating point values were incorrecting surrounding the
  value with quotes.  These quotes have been removed.  Any consumer of structured
  JSON output that was consuming the floating point values was previously having
  to interpret the quoted string and will most likely need to be fixed to take
  the unquoted number.

Notable Changes
---------------

* arch: fix NEON feaeture detection (#10185 Loic Dachary)
* build: adjust build deps for yasm, virtualenv (Jianpeng Ma)
* build: improve build dependency tooling (Loic Dachary)
* ceph-disk: call partx/partprobe consistency (#9721 Loic Dachary)
* ceph-disk: fix dmcrypt key permissions (Loic Dachary)
* ceph-disk: fix umount race condition (#10096 Blaine Gardner)
* ceph-disk: init=none option (Loic Dachary)
* ceph-monstore-tool: fix shutdown (#10093 Loic Dachary)
* ceph-objectstore-tool: fix import (#10090 David Zafman)
* ceph-objectstore-tool: many improvements and tests (David Zafman)
* ceph.spec: package rbd-replay-prep (Ken Dreyer)
* common: add 'perf reset ...' admin command (Jianpeng Ma)
* common: do not unlock rwlock on destruction (Federico Simoncelli)
* common: fix block device discard check (#10296 Sage Weil)
* common: remove broken CEPH_LOCKDEP optoin (Kefu Chai)
* crush: fix tree bucket behavior (Rongze Zhu)
* doc: add build-doc guidlines for Fedora and CentOS/RHEL (Nilamdyuti Goswami)
* doc: enable rbd cache on openstack deployments (Sebastien Han)
* doc: improved installation nots on CentOS/RHEL installs (John Wilkins)
* doc: misc cleanups (Adam Spiers, Sebastien Han, Nilamdyuti Goswami, Ken Dreyer, John Wilkins)
* doc: new man pages (Nilamdyuti Goswami)
* doc: update release descriptions (Ken Dreyer)
* doc: update sepia hardware inventory (Sandon Van Ness)
* librados: only export public API symbols (Jason Dillaman)
* libradosstriper: fix stat strtoll (Dongmao Zhang)
* libradosstriper: fix trunc method (#10129 Sebastien Ponce)
* librbd: fix list_children from invalid pool ioctxs (#10123 Jason Dillaman)
* librbd: only export public API symbols (Jason Dillaman)
* many coverity fixes (Danny Al-Gaaf)
* mds: 'flush journal' admin command (John Spray)
* mds: fix MDLog IO callback deadlock (John Spray)
* mds: fix deadlock during journal probe vs purge (#10229 Yan, Zheng)
* mds: fix race trimming log segments (Yan, Zheng)
* mds: store backtrace for stray dir (Yan, Zheng)
* mds: verify backtrace when fetching dirfrag (#9557 Yan, Zheng)
* mon: add max pgs per osd warning (Sage Weil)
* mon: fix *_ratio* units and types (Sage Weil)
* mon: fix JSON dumps to dump floats as flots and not strings (Sage Weil)
* mon: fix formatter 'pg stat' command output (Sage Weil)
* msgr: async: several fixes (Haomai Wang)
* msgr: simple: fix rare deadlock (Greg Farnum)
* osd: batch pg log trim (Xinze Chi)
* osd: clean up internal ObjectStore interface (Sage Weil)
* osd: do not abort deep scrub on missing hinfo (#10018 Loic Dachary)
* osd: fix ghobject_t formatted output to include shard (#10063 Loic Dachary)
* osd: fix osd peer check on scrub messages (#9555 Sage Weil)
* osd: fix pgls filter ops (#9439 David Zafman)
* osd: flush snapshots from cache tier immediately (Sage Weil)
* osd: keyvaluestore: fix getattr semantics (Haomai Wang)
* osd: keyvaluestore: fix key ordering (#10119 Haomai Wang)
* osd: limit in-flight read requests (Jason Dillaman)
* osd: log when scrub or repair starts (Loic Dachary)
* osd: support for discard for journal trim (Jianpeng Ma)
* qa: fix osd create dup tests (#10083 Loic Dachary)
* rgw: add location header when object is in another region (VRan Liu)
* rgw: check timestamp on s3 keystone auth (#10062 Abhishek Lekshmanan)
* rgw: make sysvinit script set ulimit -n properly (Sage Weil)
* systemd: better systemd unit files (Owen Synge)
* tests: ability to run unit tests under docker (Loic Dachary)


v0.89
=====

This is the second development release since Giant.  The big items
include the first batch of scrub patchs from Greg for CephFS, a rework
in the librados object listing API to properly handle namespaces, and
a pile of bug fixes for RGW.  There are also several smaller issues
fixed up in the performance area with buffer alignment and memory
copies, osd cache tiering agent, and various CephFS fixes.

Upgrading
---------

* New ability to list all objects from all namespaces can fail or
  return incomplete results when not all OSDs have been upgraded.
  Features rados --all ls, rados cppool, rados export, rados
  cache-flush-evict-all and rados cache-try-flush-evict-all can also
  fail or return incomplete results.

Notable Changes
---------------

* buffer: add list::get_contiguous (Sage Weil)
* buffer: avoid rebuild if buffer already contiguous (Jianpeng Ma)
* ceph-disk: improved systemd support (Owen Synge)
* ceph-disk: set guid if reusing journal partition (Dan van der Ster)
* ceph-fuse, libcephfs: allow xattr caps in inject_release_failure (#9800 John Spray)
* ceph-fuse, libcephfs: fix I_COMPLETE_ORDERED checks (#9894 Yan, Zheng)
* ceph-fuse: fix dentry invalidation on 3.18+ kernels (#9997 Yan, Zheng)
* crush: fix detach_bucket (#10095 Sage Weil)
* crush: fix several bugs in adjust_item_weight (Rongze Zhu)
* doc: add dumpling to firefly upgrade section (#7679 John Wilkins)
* doc: document erasure coded pool operations (#9970 Loic Dachary)
* doc: file system osd config settings (Kevin Dalley)
* doc: key/value store config reference (John Wilkins)
* doc: update openstack docs for Juno (Sebastien Han)
* fix cluster logging from non-mon daemons (Sage Weil)
* init-ceph: check for systemd-run before using it (Boris Ranto)
* librados: fix infinite loop with skipped map epochs (#9986 Ding Dinghua)
* librados: fix iterator operator= bugs (#10082 David Zafman, Yehuda Sadeh)
* librados: fix null deref when pool DNE (#9944 Sage Weil)
* librados: fix timer race from recent refactor (Sage Weil)
* libradosstriper: fix shutdown hang (Dongmao Zhang)
* librbd: don't close a closed parent in failure path (#10030 Jason Dillaman)
* librbd: fix diff test (#10002 Josh Durgin)
* librbd: fix locking for readahead (#10045 Jason Dillaman)
* librbd: refactor unit tests to use fixtures (Jason Dillaman)
* many many coverity cleanups (Danny Al-Gaaf)
* mds: a whole bunch of initial scrub infrastructure (Greg Farnum)
* mds: fix compat_version for MClientSession (#9945 John Spray)
* mds: fix reply snapbl (Yan, Zheng)
* mon: allow adding tiers to fs pools (#10135 John Spray)
* mon: fix MDS health status from peons (#10151 John Spray)
* mon: fix caching for min_last_epoch_clean (#9987 Sage Weil)
* mon: fix error output for add_data_pool (#9852 Joao Eduardo Luis)
* mon: include entity name in audit log for forwarded requests (#9913 Joao Eduardo Luis)
* mon: paxos: allow reads while proposing (#9321 #9322 Joao Eduardo Luis)
* msgr: asyncmessenger: add kqueue support (#9926 Haomai Wang)
* osd, librados: revamp PG listing API to handle namespaces (#9031 #9262 #9438 David Zafman)
* osd, mon: send intiial pg create time from mon to osd (#9887 David Zafman)
* osd: allow whiteout deletion in cache pool (Sage Weil)
* osd: cache pool: ignore min flush age when cache is full (Xinze Chi)
* osd: erasure coding: allow bench.sh to test ISA backend (Yuan Zhou)
* osd: erasure-code: encoding regression tests, corpus (#9420 Loic Dachary)
* osd: fix journal shutdown race (Sage Weil)
* osd: fix object age eviction (Zhiqiang Wang)
* osd: fix object atime calculation (Xinze Chi)
* osd: fix past_interval display bug (#9752 Loic Dachary)
* osd: journal: fix alignment checks, avoid useless memmove (Jianpeng Ma)
* osd: journal: update committed_thru after replay (#6756 Samuel Just)
* osd: keyvaluestore_dev: optimization (Chendi Xue)
* osd: make misdirected op checks robust for EC pools (#9835 Sage Weil)
* osd: removed some dead code (Xinze Chi)
* qa: parallelize make check (Loic Dachary)
* qa: tolerate nearly-full disk for make check (Loic Dachary)
* rgw: create subuser if needed when creating user (#10103 Yehuda Sadeh)
* rgw: fix If-Modified-Since (VRan Liu)
* rgw: fix content-length update (#9576 Yehuda Sadeh)
* rgw: fix disabling of max_size quota (#9907 Dong Lei)
* rgw: fix incorrect len when len is 0 (#9877 Yehuda Sadeh)
* rgw: fix object copy content type (#9478 Yehuda Sadeh)
* rgw: fix user stags in get-user-info API (#9359 Ray Lv)
* rgw: remove swift user manifest (DLO) hash calculation (#9973 Yehuda Sadeh)
* rgw: return timestamp on GET/HEAD (#8911 Yehuda Sadeh)
* rgw: set ETag on object copy (#9479 Yehuda Sadeh)
* rgw: update bucket index on attr changes, for multi-site sync (#5595 Yehuda Sadeh)


v0.88
=====

This is the first development release after Giant.  The two main
features merged this round are the new AsyncMessenger (an alternative
implementation of the network layer) from Haomai Wang at UnitedStack,
and support for POSIX file locks in ceph-fuse and libcephfs from Yan,
Zheng.  There is also a big pile of smaller items that re merged while
we were stabilizing Giant, including a range of smaller performance
and bug fixes and some new tracepoints for LTTNG.

Notable Changes
---------------

* ceph-disk: Scientific Linux support (Dan van der Ster)
* ceph-disk: respect --statedir for keyring (Loic Dachary)
* ceph-fuse, libcephfs: POSIX file lock support (Yan, Zheng)
* ceph-fuse, libcephfs: fix cap flush overflow (Greg Farnum, Yan, Zheng)
* ceph-fuse, libcephfs: fix root inode xattrs (Yan, Zheng)
* ceph-fuse, libcephfs: preserve dir ordering (#9178 Yan, Zheng)
* ceph-fuse, libcephfs: trim inodes before reconnecting to MDS (Yan, Zheng)
* ceph: do not parse injectargs twice (Loic Dachary)
* ceph: make 'ceph -s' output more readable (Sage Weil)
* ceph: new 'ceph tell mds.$name_or_rank_or_gid' (John Spray)
* ceph: test robustness (Joao Eduardo Luis)
* ceph_objectstore_tool: behave with sharded flag (#9661 David Zafman)
* cephfs-journal-tool: fix journal import (#10025 John Spray)
* cephfs-journal-tool: skip up to expire_pos (#9977 John Spray)
* cleanup rados.h definitions with macros (Ilya Dryomov)
* common: shared_cache unit tests (Cheng Cheng)
* config: add $cctid meta variable (Adam Crume)
* crush: fix buffer overrun for poorly formed rules (#9492 Johnu George)
* crush: improve constness (Loic Dachary)
* crushtool: add --location <id> command (Sage Weil, Loic Dachary)
* default to libnss instead of crypto++ (Federico Gimenez)
* doc: ceph osd reweight vs crush weight (Laurent Guerby)
* doc: document the LRC per-layer plugin configuration (Yuan Zhou)
* doc: erasure code doc updates (Loic Dachary)
* doc: misc updates (Alfredo Deza, VRan Liu)
* doc: preflight doc fixes (John Wilkins)
* doc: update PG count guide (Gerben Meijer, Laurent Guerby, Loic Dachary)
* keyvaluestore: misc fixes (Haomai Wang)
* keyvaluestore: performance improvements (Haomai Wang)
* librados: add rados_pool_get_base_tier() call (Adam Crume)
* librados: cap buffer length (Loic Dachary)
* librados: fix objecter races (#9617 Josh Durgin)
* libradosstriper: misc fixes (Sebastien Ponce)
* librbd: add missing python docstrings (Jason Dillaman)
* librbd: add readahead (Adam Crume)
* librbd: fix cache tiers in list_children and snap_unprotect (Adam Crume)
* librbd: fix performance regression in ObjectCacher (#9513 Adam Crume)
* librbd: lttng tracepoints (Adam Crume)
* librbd: misc fixes (Xinxin Shu, Jason Dillaman)
* mds: fix sessionmap lifecycle bugs (Yan, Zheng)
* mds: initialize root inode xattr version (Yan, Zheng)
* mds: introduce auth caps (John Spray)
* mds: misc bugs (Greg Farnum, John Spray, Yan, Zheng, Henry Change)
* misc coverity fixes (Danny Al-Gaaf)
* mon: add 'ceph osd rename-bucket ...' command (Loic Dachary)
* mon: clean up auth list output (Loic Dachary)
* mon: fix 'osd crush link' id resolution (John Spray)
* mon: fix misc error paths (Joao Eduardo Luis)
* mon: fix paxos off-by-one corner case (#9301 Sage Weil)
* mon: new 'ceph pool ls [detail]' command (Sage Weil)
* mon: wait for writeable before cross-proposing (#9794 Joao Eduardo Luis)
* msgr: avoid useless new/delete (Haomai Wang)
* msgr: fix delay injection bug (#9910 Sage Weil, Greg Farnum)
* msgr: new AsymcMessenger alternative implementation (Haomai Wang)
* msgr: prefetch data when doing recv (Yehuda Sadeh)
* osd: add erasure code corpus (Loic Dachary)
* osd: add misc tests (Loic Dachary, Danny Al-Gaaf)
* osd: cleanup boost optionals (William Kennington)
* osd: expose non-journal backends via ceph-osd CLI (Hoamai Wang)
* osd: fix JSON output for stray OSDs (Loic Dachary)
* osd: fix ioprio options (Loic Dachary)
* osd: fix transaction accounting (Jianpeng Ma)
* osd: misc optimizations (Xinxin Shu, Zhiqiang Wang, Xinze Chi)
* osd: use FIEMAP_FLAGS_SYNC instead of fsync (Jianpeng Ma)
* rados: fix put of /dev/null (Loic Dachary)
* rados: parse command-line arguments more strictly (#8983 Adam Crume)
* rbd-fuse: fix memory leak (Adam Crume)
* rbd-replay-many (Adam Crume)
* rbd-replay: --anonymize flag to rbd-replay-prep (Adam Crume)
* rbd: fix 'rbd diff' for non-existent objects (Adam Crume)
* rbd: fix error when striping with format 1 (Sebastien Han)
* rbd: fix export for image sizes over 2GB (Vicente Cheng)
* rbd: use rolling average for rbd bench-write throughput (Jason Dillaman)
* rgw: send explicit HTTP status string (Yehuda Sadeh)
* rgw: set length for keystone token validation request (#7796 Yehuda Sadeh, Mark Kirkwood)
* udev: fix rules for CentOS7/RHEL7 (Loic Dachary)
* use clock_gettime instead of gettimeofday (Jianpeng Ma)
* vstart.sh: set up environment for s3-tests (Luis Pabon)


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

For more detailed information, see :download:`the complete changelog <changelog/v0.87.2.txt>`.

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

For more detailed information, see :download:`the complete changelog <changelog/v0.87.1.txt>`.



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


v0.80.9 Firefly
===============

This is a bugfix release for firefly.  It fixes a performance
regression in librbd, an important CRUSH misbehavior (see below), and
several RGW bugs.  We have also backported support for flock/fcntl
locks to ceph-fuse and libcephfs.

We recommend that all Firefly users upgrade.

For more detailed information, see :download:`the complete changelog <changelog/v0.80.9.txt>`.

Adjusting CRUSH maps
--------------------

* This point release fixes several issues with CRUSH that trigger
  excessive data migration when adjusting OSD weights.  These are most
  obvious when a very small weight change (e.g., a change from 0 to
  .01) triggers a large amount of movement, but the same set of bugs
  can also lead to excessive (though less noticeable) movement in
  other cases.

  However, because the bug may already have affected your cluster,
  fixing it may trigger movement *back* to the more correct location.
  For this reason, you must manually opt-in to the fixed behavior.

  In order to set the new tunable to correct the behavior::

     ceph osd crush set-tunable straw_calc_version 1

  Note that this change will have no immediate effect.  However, from
  this point forward, any 'straw' bucket in your CRUSH map that is
  adjusted will get non-buggy internal weights, and that transition
  may trigger some rebalancing.

  You can estimate how much rebalancing will eventually be necessary
  on your cluster with::

     ceph osd getcrushmap -o /tmp/cm
     crushtool -i /tmp/cm --num-rep 3 --test --show-mappings > /tmp/a 2>&1
     crushtool -i /tmp/cm --set-straw-calc-version 1 -o /tmp/cm2
     crushtool -i /tmp/cm2 --reweight -o /tmp/cm2
     crushtool -i /tmp/cm2 --num-rep 3 --test --show-mappings > /tmp/b 2>&1
     wc -l /tmp/a                          # num total mappings
     diff -u /tmp/a /tmp/b | grep -c ^+    # num changed mappings

   Divide the total number of lines in /tmp/a with the number of lines
   changed.  We've found that most clusters are under 10%.

   You can force all of this rebalancing to happen at once with::

     ceph osd crush reweight-all

   Otherwise, it will happen at some unknown point in the future when
   CRUSH weights are next adjusted.

Notable Changes
---------------

* ceph-fuse: flock, fcntl lock support (Yan, Zheng, Greg Farnum)
* crush: fix straw bucket weight calculation, add straw_calc_version tunable (#10095 Sage Weil)
* crush: fix tree bucket (Rongzu Zhu)
* crush: fix underflow of tree weights (Loic Dachary, Sage Weil)
* crushtool: add --reweight (Sage Weil)
* librbd: complete pending operations before losing image (#10299 Jason Dillaman)
* librbd: fix read caching performance regression (#9854 Jason Dillaman)
* librbd: gracefully handle deleted/renamed pools (#10270 Jason Dillaman)
* mon: fix dump of chooseleaf_vary_r tunable (Sage Weil)
* osd: fix PG ref leak in snaptrimmer on peering (#10421 Kefu Chai)
* osd: handle no-op write with snapshot (#10262 Sage Weil)
* radosgw-admin: create subuser when creating user (#10103 Yehuda Sadeh)
* rgw: change multipart uplaod id magic (#10271 Georgio Dimitrakakis, Yehuda Sadeh)
* rgw: don't overwrite bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: enable IPv6 for embedded civetweb (#10965 Yehuda Sadeh)
* rgw: fix partial swift GET (#10553 Yehuda Sadeh)
* rgw: fix quota disable (#9907 Dong Lei)
* rgw: index swift keys appropriately (#10471 Hemant Burman, Yehuda Sadeh)
* rgw: make setattrs update bucket index (#5595 Yehuda Sadeh)
* rgw: pass civetweb configurables (#10907 Yehuda Sadeh)
* rgw: remove swift user manifest (DLO) hash calculation (#9973 Yehuda Sadeh)
* rgw: return correct len for 0-len objects (#9877 Yehuda Sadeh)
* rgw: S3 object copy content-type fix (#9478 Yehuda Sadeh)
* rgw: send ETag on S3 object copy (#9479 Yehuda Sadeh)
* rgw: send HTTP status reason explicitly in fastcgi (Yehuda Sadeh)
* rgw: set ulimit -n from sysvinit (el6) init script (#9587 Sage Weil)
* rgw: update swift subuser permission masks when authenticating (#9918 Yehuda Sadeh)
* rgw: URL decode query params correctly (#10271 Georgio Dimitrakakis, Yehuda Sadeh)
* rgw: use attrs when reading object attrs (#10307 Yehuda Sadeh)
* rgw: use \r\n for http headers (#9254 Benedikt Fraunhofer, Yehuda Sadeh)


v0.80.8 Firefly
===============

This is a long-awaited bugfix release for firefly.  It has several
imporant (but relatively rare) OSD peering fixes, performance issues
when snapshots are trimmed, several RGW fixes, a paxos corner case
fix, and some packaging updates.

We recommend that all users for v0.80.x firefly upgrade when it is
convenient to do so.

For more detailed information, see :download:`the complete changelog <changelog/v0.80.8.txt>`.

Notable Changes
---------------

* build: remove stack-execute bit from assembled code sections (#10114 Dan Mick)
* ceph-disk: fix dmcrypt key permissions (#9785 Loic Dachary)
* ceph-disk: fix keyring location (#9653 Loic Dachary)
* ceph-disk: make partition checks more robust (#9721 #9665 Loic Dachary)
* ceph: cleanly shut down librados context on shutdown (#8797 Dan Mick)
* common: add $cctid config metavariable (#6228 Adam Crume)
* crush: align rule and ruleset ids (#9675 Xiaoxi Chen)
* crush: fix negative weight bug during create_or_move_item (#9998 Pawel Sadowski)
* crush: fix potential buffer overflow in erasure rules (#9492 Johnu George)
* debian: fix python-ceph -> ceph file movement (Sage Weil)
* libcephfs,ceph-fuse: fix flush tid wraparound bug (#9869 Greg Farnum, Yan, Zheng)
* libcephfs: close fd befure umount (#10415 Yan, Zheng)
* librados: fix crash from C API when read timeout is enabled (#9582 Sage Weil)
* librados: handle reply race with pool deletion (#10372 Sage Weil)
* librbd: cap memory utilization for read requests (Jason Dillaman)
* librbd: do not close a closed parent image on failure (#10030 Jason Dillaman)
* librbd: fix diff tests (#10002 Josh Durgin)
* librbd: protect list_children from invalid pools (#10123 Jason Dillaman)
* make check improvemens (Loic Dachary)
* mds: fix ctime updates (#9514 Greg Farnum)
* mds: fix journal import tool (#10025 John Spray)
* mds: fix rare NULL deref in cap flush handler (Greg Farnum)
* mds: handle unknown lock messages (Yan, Zheng)
* mds: store backtrace for straydir (Yan, Zheng)
* mon: abort startup if disk is full (#9502 Joao Eduardo Luis)
* mon: add paxos instrumentation (Sage Weil)
* mon: fix double-free in rare OSD startup path (Sage Weil)
* mon: fix osdmap trimming (#9987 Sage Weil)
* mon: fix paxos corner cases (#9301 #9053 Sage Weil)
* osd: cancel callback on blacklisted watchers (#8315 Samuel Just)
* osd: cleanly abort set-alloc-hint operations during upgrade (#9419 David Zafman)
* osd: clear rollback PG metadata on PG deletion (#9293 Samuel Just)
* osd: do not abort deep scrub if hinfo is missing (#10018 Loic Dachary)
* osd: erasure-code regression tests (Loic Dachary)
* osd: fix distro metadata reporting for SUSE (#8654 Danny Al-Gaaf)
* osd: fix full OSD checks during backfill (#9574 Samuel Just)
* osd: fix ioprio parsing (#9677 Loic Dachary)
* osd: fix journal direct-io shutdown (#9073 Mark Kirkwood, Ma Jianpeng, Somnath Roy)
* osd: fix journal dump (Ma Jianpeng)
* osd: fix occasional stall during peering or activation (Sage Weil)
* osd: fix past_interval display bug (#9752 Loic Dachary)
* osd: fix rare crash triggered by admin socket dump_ops_in_filght (#9916 Dong Lei)
* osd: fix snap trimming performance issues (#9487 #9113 Samuel Just, Sage Weil, Dan van der Ster, Florian Haas)
* osd: fix snapdir handling on cache eviction (#8629 Sage Weil)
* osd: handle map gaps in map advance code (Sage Weil)
* osd: handle undefined CRUSH results in interval check (#9718 Samuel Just)
* osd: include shard in JSON dump of ghobject (#10063 Loic Dachary)
* osd: make backfill reservation denial handling more robust (#9626 Samuel Just)
* osd: make misdirected op checks handle EC + primary affinity (#9835 Samuel Just, Sage Weil)
* osd: mount XFS with inode64 by default (Sage Weil)
* osd: other misc bugs (#9821 #9875 Samuel Just)
* rgw: add .log to default log path (#9353 Alexandre Marangone)
* rgw: clean up fcgi request context (#10194 Yehuda Sadeh)
* rgw: convet header underscores to dashes (#9206 Yehuda Sadeh)
* rgw: copy object data if copy target is in different pool (#9039 Yehuda Sadeh)
* rgw: don't try to authenticate CORS peflight request (#8718 Robert Hubbard, Yehuda Sadeh)
* rgw: fix civetweb URL decoding (#8621 Yehuda Sadeh)
* rgw: fix hash calculation during PUT (Yehuda Sadeh)
* rgw: fix misc bugs (#9089 #9201 Yehuda Sadeh)
* rgw: fix object tail test (#9226 Sylvain Munaut, Yehuda Sadeh)
* rgw: make sysvinit script run rgw under systemd context as needed (#10125 Loic Dachary)
* rgw: separate civetweb log from rgw log (Yehuda Sadeh)
* rgw: set length for keystone token validations (#7796 Mark Kirkwood, Yehuda Sadeh)
* rgw: subuser creation fixes (#8587 Yehuda Sadeh)
* rpm: misc packaging improvements (Sandon Van Ness, Dan Mick, Erik Logthenberg, Boris Ranto)
* rpm: use standard udev rules for CentOS7/RHEL7 (#9747 Loic Dachary)


v0.80.7 Firefly
===============

This release fixes a few critical issues with v0.80.6, particularly
with clusters running mixed versions.

We recommend that all v0.80.x Firefly users upgrade to this release.

For more detailed information, see :download:`the complete changelog <changelog/v0.80.7.txt>`.

Notable Changes
---------------

* osd: fix invalid memory reference in log trimming (#9731 Samuel Just)
* osd: fix use-after-free in cache tiering code (#7588 Sage Weil)
* osd: remove bad backfill assertion for mixed-version clusters (#9696 Samuel Just)



v0.80.6 Firefly
===============

This is a major bugfix release for firefly, fixing a range of issues
in the OSD and monitor, particularly with cache tiering.  There are
also important fixes in librados, with the watch/notify mechanism used
by librbd, and in radosgw.

A few pieces of new functionality of been backported, including improved
'ceph df' output (view amount of writeable space per pool), support for
non-default cluster names when using sysvinit or systemd, and improved
(and fixed) support for dmcrypt.

We recommend that all v0.80.x Firefly users upgrade to this release.

For more detailed information, see :download:`the complete changelog <changelog/v0.80.6.txt>`.

Notable Changes
---------------

* build: fix atomic64_t on i386 (#8969 Sage Weil)
* build: fix build on alpha (Michael Cree, Dmitry Smirnov)
* build: fix build on hppa (Dmitry Smirnov)
* build: fix yasm detection on x32 arch (Sage Weil)
* ceph-disk: fix 'list' function with dmcrypt (Sage Weil)
* ceph-disk: fix dmcrypt support (Alfredo Deza)
* ceph: allow non-default cluster to be specified (#8944)
* common: fix dup log messages to mon (#9080 Sage Weil)
* global: write pid file when -f is used (systemd, upstart) (Alexandre Oliva)
* librados: fix crash when read timeout is enabled (#9362 Matthias Kiefer, Sage Weil)
* librados: fix lock leaks in error paths (#9022 Pavan Rallabhandi)
* librados: fix watch resend on PG acting set change (#9220 Samuel Just)
* librados: python: fix aio_read handling with \0 (Mohammad Salehe)
* librbd: add interface to invalidate cached data (Josh Durgin)
* librbd: fix crash when using clone of flattened image (#8845 Josh Durgin)
* librbd: fix error path cleanup on open (#8912 Josh Durgin)
* librbd: fix null pointer check (Danny Al-Gaaf)
* librbd: limit dirty object count (Haomai Wang)
* mds: fix rstats for root and mdsdir (Yan, Zheng)
* mon: add 'get' command for new cache tier pool properties (Joao Eduardo Luis)
* mon: add 'osd pool get-quota' (#8523 Joao Eduardo Luis)
* mon: add cluster fingerprint (Sage Weil)
* mon: disallow nonsensical cache-mode transitions (#8155 Joao Eduardo Luis)
* mon: fix cache tier rounding error on i386 (Sage Weil)
* mon: fix occasional memory leak (#9176 Sage Weil)
* mon: fix reported latency for 'osd perf' (#9269 Samuel Just)
* mon: include 'max avail' in 'ceph df' output (Sage Weil, Xioaxi Chen)
* mon: persistently mark pools where scrub may find incomplete clones (#8882 Sage Weil)
* mon: preload erasure plugins (Loic Dachary)
* mon: prevent cache-specific settings on non-tier pools (#8696 Joao Eduardo Luis)
* mon: reduce log spam (Aanchal Agrawal, Sage Weil)
* mon: warn when cache pools have no hit_sets enabled (Sage Weil)
* msgr: fix trivial memory leak (Sage Weil)
* osd: automatically scrub PGs with invalid stats (#8147 Sage Weil)
* osd: avoid sharing PG metadata that is not durable (Samuel Just)
* osd: cap hit_set size (#9339 Samuel Just)
* osd: create default erasure profile if needed (#8601 Loic Dachary)
* osd: dump tid as JSON int (not string)  where appropriate (Joao Eduardo Luis)
* osd: encode blacklist in deterministic order (#9211 Sage Weil)
* osd: fix behavior when cache tier has no hit_sets enabled (#8982 Sage Weil)
* osd: fix cache tier flushing of snapshots (#9054 Samuel Just)
* osd: fix cache tier op ordering when going from full to non-full (#8931 Sage Weil)
* osd: fix crash on dup recovery reservation (#8863 Sage Weil)
* osd: fix division by zero when pg_num adjusted with no OSDs (#9052 Sage Weil)
* osd: fix hint crash in experimental keyvaluestore_dev backend (Hoamai Wang)
* osd: fix leak in copyfrom cancellation (#8894 Samuel Just)
* osd: fix locking for copyfrom finish (#8889 Sage Weil)
* osd: fix long filename handling in backend (#8701 Sage Weil)
* osd: fix min_size check with backfill (#9497 Samuel Just)
* osd: fix mount/remount sync race (#9144 Sage Weil)
* osd: fix object listing + erasure code bug (Guang Yang)
* osd: fix race on reconnect to failed OSD (#8944 Greg Farnum)
* osd: fix recovery reservation deadlock (Samuel Just)
* osd: fix tiering agent arithmetic for negative values (#9082 Karan Singh)
* osd: improve shutdown order (#9218 Sage Weil)
* osd: improve subop discard logic (#9259 Samuel Just)
* osd: introduce optional sleep, io priority for scrub and snap trim (Sage Weil)
* osd: make scrub check for and remove stale erasure-coded objects (Samuel Just)
* osd: misc fixes (#9481 #9482 #9179 Sameul Just)
* osd: mix keyvaluestore_dev improvements (Haomai Wang)
* osd: only require CRUSH features for rules that are used (#8963 Sage Weil)
* osd: preload erasure plugins on startup (Loic Dachary)
* osd: prevent PGs from falling behind when consuming OSDMaps (#7576 Sage Weil)
* osd: prevent old clients from using tiered pools (#8714 Sage Weil)
* osd: set min_size on erasure pools to data chunk count (Sage Weil)
* osd: trim old erasure-coded objects more aggressively (Samuel Just)
* rados: enforce erasure code alignment (Lluis Pamies-Juarez)
* rgw: align object stripes with erasure pool alignment (#8442 Yehuda Sadeh)
* rgw: don't send error body on HEAD for civetweb (#8539 Yehuda Sadeh)
* rgw: fix crash in CORS preflight request (Yehuda Sadeh)
* rgw: fix decoding of + in URL (#8702 Brian Rak)
* rgw: fix object removal on object create (#8972 Patrycja Szabowska, Yehuda Sadeh)
* systemd: use systemd-run when starting radosgw (JuanJose Galvez)
* sysvinit: support non-default cluster name (Alfredo Deza)


v0.80.5 Firefly
===============

This release fixes a few important bugs in the radosgw and fixes
several packaging and environment issues, including OSD log rotation,
systemd environments, and daemon restarts on upgrade.

We recommend that all v0.80.x Firefly users upgrade, particularly if they
are using upstart, systemd, or radosgw.

Notable Changes
---------------

* ceph-dencoder: do not needlessly link to librgw, librados, etc. (Sage Weil)
* do not needlessly link binaries to leveldb (Sage Weil)
* mon: fix mon crash when no auth keys are present (#8851, Joao Eduardo Luis)
* osd: fix cleanup (and avoid occasional crash) during shutdown (#7981, Sage Weil)
* osd: fix log rotation under upstart (Sage Weil)
* rgw: fix multipart upload when object has irregular size (#8846, Yehuda Sadeh, Sylvain Munaut)
* rgw: improve bucket listing S3 compatibility (#8858, Yehuda Sadeh)
* rgw: improve delimited bucket listing (Yehuda Sadeh)
* rpm: do not restart daemons on upgrade (#8849, Alfredo Deza)

For more detailed information, see :download:`the complete changelog <changelog/v0.80.5.txt>`.

v0.80.4 Firefly
===============

This Firefly point release fixes an potential data corruption problem
when ceph-osd daemons run on top of XFS and service Firefly librbd
clients.  A recently added allocation hint that RBD utilizes triggers
an XFS bug on some kernels (Linux 3.2, and likely others) that leads
to data corruption and deep-scrub errors (and inconsistent PGs).  This
release avoids the situation by disabling the allocation hint until we
can validate which kernels are affected and/or are known to be safe to
use the hint on.

We recommend that all v0.80.x Firefly users urgently upgrade,
especially if they are using RBD.

Notable Changes
---------------

* osd: disable XFS extsize hint by default (#8830, Samuel Just)
* rgw: fix extra data pool default name (Yehuda Sadeh)

For more detailed information, see :download:`the complete changelog <changelog/v0.80.4.txt>`.


v0.80.3 Firefly
===============

This is the third Firefly point release.  It includes a single fix
for a radosgw regression that was discovered in v0.80.2 right after it
was released.

We recommand that all v0.80.x Firefly users upgrade.

Notable Changes
---------------

* radosgw: fix regression in manifest decoding (#8804, Sage Weil)

For more detailed information, see :download:`the complete changelog <changelog/v0.80.3.txt>`.


v0.80.2 Firefly
===============

This is the second Firefly point release.  It contains a range of
important fixes, including several bugs in the OSD cache tiering, some
compatibility checks that affect upgrade situations, several radosgw
bugs, and an irritating and unnecessary feature bit check that
prevents older clients from communicating with a cluster with any
erasure coded pools.

One someone large change in this point release is that the ceph RPM
package is separated into a ceph and ceph-common package, similar to
Debian.  The ceph-common package contains just the client libraries
without any of the server-side daemons.

We recommend that all v0.80.x Firefly users skip this release and use
v0.80.3.

Notable Changes
---------------

* ceph-disk: better debug logging (Alfredo Deza)
* ceph-disk: fix preparation of OSDs with dmcrypt (#6700, Stephen F Taylor)
* ceph-disk: partprobe on prepare to fix dm-crypt (#6966, Eric Eastman)
* do not require ERASURE_CODE feature from clients (#8556, Sage Weil)
* libcephfs-java: build with older JNI headers (Greg Farnum)
* libcephfs-java: fix build with gcj-jdk (Dmitry Smirnov)
* librados: fix osd op tid for redirected ops (#7588, Samuel Just)
* librados: fix rados_pool_list buffer bounds checks (#8447, Sage Weil)
* librados: resend ops when pool overlay changes (#8305, Sage Weil)
* librbd, ceph-fuse: reduce CPU overhead for clean object check in cache (Haomai Wang)
* mon: allow deletion of cephfs pools (John Spray)
* mon: fix default pool ruleset choice (#8373, John Spray)
* mon: fix health summary for mon low disk warning (Sage Weil)
* mon: fix 'osd pool set <pool> cache_target_full_ratio' (Geoffrey Hartz)
* mon: fix quorum feature check (Greg Farnum)
* mon: fix request forwarding in mixed firefly+dumpling clusters 9#8727, Joao Eduardo Luis)
* mon: fix rule vs ruleset check in 'osd pool set ... crush_ruleset' command (John Spray)
* mon: make osd 'down' count accurate (Sage Weil)
* mon: set 'next commit' in primary-affinity reply (Ilya Dryomov)
* mon: verify CRUSH features are supported by all mons (#8738, Greg Farnum)
* msgr: fix sequence negotiation during connection reset (Guang Yang)
* osd: block scrub on blocked objects (#8011, Samuel Just)
* osd: call XFS hint ioctl less often (#8241, Ilya Dryomov)
* osd: copy xattr spill out marker on clone (Haomai Wang)
* osd: fix flush of snapped objects (#8334, Samuel Just)
* osd: fix hashindex restart of merge operation (#8332, Samuel Just)
* osd: fix osdmap subscription bug causing startup hang (Greg Farnum)
* osd: fix potential null deref (#8328, Sage Weil)
* osd: fix shutdown race (#8319, Sage Weil)
* osd: handle 'none' in CRUSH results properly during peering (#8507, Samuel Just)
* osd: set no spill out marker on new objects (Greg Farnum)
* osd: skip op ordering debug checks on tiered pools (#8380, Sage Weil)
* rados: enforce 'put' alignment (Lluis Pamies-Juarez)
* rest-api: fix for 'rx' commands (Ailing Zhang)
* rgw: calc user manifest etag and fix check (#8169, #8436, Yehuda Sadeh)
* rgw: fetch attrs on multipart completion (#8452, Yehuda Sadeh, Sylvain Munaut)
* rgw: fix buffer overflow for long instance ids (#8608, Yehuda Sadeh)
* rgw: fix entity permission check on metadata put (#8428, Yehuda Sadeh)
* rgw: fix multipart retry race (#8269, Yehuda Sadeh)
* rpm: split ceph into ceph and ceph-common RPMs (Sandon Van Ness, Dan Mick)
* sysvinit: continue startin daemons after failure doing mount (#8554, Sage Weil)

For more detailed information, see :download:`the complete changelog <changelog/v0.80.2.txt>`.

v0.80.1 Firefly
===============

This first Firefly point release fixes a few bugs, the most visible
being a problem that prevents scrub from completing in some cases.

Notable Changes
---------------

* osd: revert incomplete scrub fix (Samuel Just)
* rgw: fix stripe calculation for manifest objects (Yehuda Sadeh)
* rgw: improve handling, memory usage for abort reads (Yehuda Sadeh)
* rgw: send Swift user manifest HTTP header (Yehuda Sadeh)
* libcephfs, ceph-fuse: expose MDS session state via admin socket (Yan, Zheng)
* osd: add simple throttle for snap trimming (Sage Weil)
* monclient: fix possible hang from ill-timed monitor connection failure (Sage Weil)
* osd: fix trimming of past HitSets (Sage Weil)
* osd: fix whiteouts for non-writeback cache modes (Sage Weil)
* osd: prevent divide by zero in tiering agent (David Zafman)
* osd: prevent busy loop when tiering agent can do no work (David Zafman)

For more detailed information, see :download:`the complete changelog <changelog/v0.80.1.txt>`.


v0.80 Firefly
=============

This release will form the basis for our long-term supported release
Firefly, v0.80.x.  The big new features are support for erasure coding
and cache tiering, although a broad range of other features, fixes,
and improvements have been made across the code base.  Highlights include:

* *Erasure coding*: support for a broad range of erasure codes for lower
  storage overhead and better data durability.
* *Cache tiering*: support for creating 'cache pools' that store hot,
  recently accessed objects with automatic demotion of colder data to
  a base tier.  Typically the cache pool is backed by faster storage
  devices like SSDs.
* *Primary affinity*: Ceph now has the ability to skew selection of
  OSDs as the "primary" copy, which allows the read workload to be
  cheaply skewed away from parts of the cluster without migrating any
  data.
* *Key/value OSD backend* (experimental): An alternative storage backend
  for Ceph OSD processes that puts all data in a key/value database like
  leveldb.  This provides better performance for workloads dominated by
  key/value operations (like radosgw bucket indices).
* *Standalone radosgw* (experimental): The radosgw process can now run
  in a standalone mode without an apache (or similar) web server or
  fastcgi.  This simplifies deployment and can improve performance.

We expect to maintain a series of stable releases based on v0.80
Firefly for as much as a year.  In the meantime, development of Ceph
continues with the next release, Giant, which will feature work on the
CephFS distributed file system, more alternative storage backends
(like RocksDB and f2fs), RDMA support, support for pyramid erasure
codes, and additional functionality in the block device (RBD) like
copy-on-read and multisite mirroring.


Upgrade Sequencing
------------------

* If your existing cluster is running a version older than v0.67
  Dumpling, please first upgrade to the latest Dumpling release before
  upgrading to v0.80 Firefly.  Please refer to the `Dumpling upgrade`_
  documentation.

* We recommand adding the following to the [mon] section of your
  ceph.conf prior to upgrade::

    mon warn on legacy crush tunables = false

  This will prevent health warnings due to the use of legacy CRUSH
  placement.  Although it is possible to rebalance existing data
  across your cluster (see the upgrade notes below), we do not
  normally recommend it for production environments as a large amount
  of data will move and there is a significant performance impact from
  the rebalancing.

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

* OSDMap's json-formatted dump changed for keys 'full' and 'nearfull'.
  What was previously being outputted as 'true' or 'false' strings are
  now being outputted 'true' and 'false' booleans according to json syntax.

* HEALTH_WARN on 'mon osd down out interval == 0'. Having this option set
  to zero on the leader acts much like having the 'noout' flag set.  This
  warning will only be reported if the monitor getting the 'health' or
  'status' request has this option set to zero.

* Monitor 'auth' commands now require the mon 'x' capability.  This matches
  dumpling v0.67.x and earlier, but differs from emperor v0.72.x.

* A librados WATCH operation on a non-existent object now returns ENOENT;
  previously it did not.

* Librados interface change:  As there are no partial writes, the rados_write()
  and rados_append() operations now return 0 on success like rados_write_full()
  always has.  This includes the C++ interface equivalents and AIO return
  values for the aio variants.

* The radosgw init script (sysvinit) how requires that the 'host = ...' line in
  ceph.conf, if present, match the short hostname (the output of 'hostname -s'),
  not the fully qualified hostname or the (occasionally non-short) output of
  'hostname'.  Failure to adjust this when upgrading from emperor or dumpling
  may prevent the radosgw daemon from starting.

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

* The default CRUSH rules and layouts are now using the 'bobtail'
  tunables and defaults.  Upgaded clusters using the old values will
  now present with a health WARN state.  This can be disabled by
  adding 'mon warn on legacy crush tunables = false' to ceph.conf and
  restarting the monitors.  Alternatively, you can switch to the new
  tunables with 'ceph osd crush tunables firefly,' but keep in mind
  that this will involve moving a *significant* portion of the data
  already stored in the cluster and in a large cluster may take
  several days to complete.  We do not recommend adjusting tunables on a
  production cluster.

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

* ceph-fuse, libcephfs: fix several caching bugs (Yan, Zheng)
* ceph-fuse: trim inodes in response to mds memory pressure (Yan, Zheng)
* librados: fix inconsistencies in API error values (David Zafman)
* librados: fix watch operations with cache pools (Sage Weil)
* librados: new snap rollback operation (David Zafman)
* mds: fix respawn (John Spray)
* mds: misc bugs (Yan, Zheng)
* mds: misc multi-mds fixes (Yan, Zheng)
* mds: use shared_ptr for requests (Greg Farnum)
* mon: fix peer feature checks (Sage Weil)
* mon: require 'x' mon caps for auth operations (Joao Luis)
* mon: shutdown when removed from mon cluster (Joao Luis)
* msgr: fix locking bug in authentication (Josh Durgin)
* osd: fix bug in journal replay/restart (Sage Weil)
* osd: many many many bug fixes with cache tiering (Samuel Just)
* osd: track omap and hit_set objects in pg stats (Samuel Just)
* osd: warn if agent cannot enable due to invalid (post-split) stats (Sage Weil)
* rados bench: track metadata for multiple runs separately (Guang Yang)
* rgw: fixed subuser modify (Yehuda Sadeh)
* rpm: fix redhat-lsb dependency (Sage Weil, Alfredo Deza)


Notable changes since v0.72 Emperor
-----------------------------------

* buffer: some zero-copy groundwork (Josh Durgin)
* build: misc improvements (Ken Dreyer)
* ceph-conf: stop creating bogus log files (Josh Durgin, Sage Weil)
* ceph-crush-location: new hook for setting CRUSH location of osd daemons on start)
* ceph-disk: avoid fd0 (Loic Dachary)
* ceph-disk: generalize path names, add tests (Loic Dachary)
* ceph-disk: misc improvements for puppet (Loic Dachary)
* ceph-disk: several bug fixes (Loic Dachary)
* ceph-fuse: fix race for sync reads (Sage Weil)
* ceph-fuse, libcephfs: fix several caching bugs (Yan, Zheng)
* ceph-fuse: trim inodes in response to mds memory pressure (Yan, Zheng)
* ceph-kvstore-tool: expanded command set and capabilities (Joao Eduardo Luis)
* ceph.spec: fix build dependency (Loic Dachary)
* common: bloom filter improvements (Sage Weil)
* common: check preexisting admin socket for active daemon before removing (Loic Dachary)
* common: fix aligned buffer allocation (Loic Dachary)
* common: fix authentication on big-endian architectures (Dan Mick)
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
* debian: change directory ownership between ceph and ceph-common (Sage Weil)
* debian: integrate misc fixes from downstream packaging (James Page)
* doc: big update to install docs (John Wilkins)
* doc: many many install doc improvements (John Wilkins)
* doc: many many updates (John Wilkins)
* doc: misc fixes (David Moreau Simard, Kun Huang)
* erasure-code: improve buffer alignment (Loic Dachary)
* erasure-code: rewrite region-xor using vector operations (Andreas Peters)
* init: fix startup ordering/timeout problem with OSDs (Dmitry Smirnov)
* libcephfs: fix resource leak (Zheng Yan)
* librados: add C API coverage for atomic write operations (Christian Marie)
* librados: fix inconsistencies in API error values (David Zafman)
* librados: fix throttle leak (and eventual deadlock) (Josh Durgin)
* librados: fix watch operations with cache pools (Sage Weil)
* librados: new snap rollback operation (David Zafman)
* librados, osd: new TMAP2OMAP operation (Yan, Zheng)
* librados: read directly into user buffer (Rutger ter Borg)
* librbd: fix use-after-free aio completion bug #5426 (Josh Durgin)
* librbd: localize/distribute parent reads (Sage Weil)
* librbd: skip zeroes/holes when copying sparse images (Josh Durgin)
* mailmap: affiliation updates (Loic Dachary)
* mailmap updates (Loic Dachary)
* many portability improvements (Noah Watkins)
* many unit test improvements (Loic Dachary)
* mds: always store backtrace in default pool (Yan, Zheng)
* mds: cope with MDS failure during creation (John Spray)
* mds: fix cap migration behavior (Yan, Zheng)
* mds: fix client session flushing (Yan, Zheng)
* mds: fix crash from client sleep/resume (Zheng Yan)
* mds: fix many many multi-mds bugs (Yan, Zheng)
* mds: fix readdir end check (Zheng Yan)
* mds: fix Resetter locking (Alexandre Oliva)
* mds: fix respawn (John Spray)
* mds: inline data support (Li Wang, Yunchuan Wen)
* mds: misc bugs (Yan, Zheng)
* mds: misc fixes for directory fragments (Zheng Yan)
* mds: misc fixes for larger directories (Zheng Yan)
* mds: misc fixes for multiple MDSs (Zheng Yan)
* mds: misc multi-mds fixes (Yan, Zheng)
* mds: remove .ceph directory (John Spray)
* mds: store directories in omap instead of tmap (Yan, Zheng)
* mds: update old-format backtraces opportunistically (Zheng Yan)
* mds: use shared_ptr for requests (Greg Farnum)
* misc cleanups from coverity (Xing Lin)
* misc coverity fixes, cleanups (Danny Al-Gaaf)
* misc coverity fixes (Xing Lin, Li Wang, Danny Al-Gaaf)
* misc portability fixes (Noah Watkins, Alan Somers)
* misc portability fixes (Noah Watkins, Christophe Courtaut, Alan Somers, huanjun)
* misc portability work (Noah Watkins)
* mon: add erasure profiles and improve erasure pool creation (Loic Dachary)
* mon: add 'mon getmap EPOCH' (Joao Eduardo Luis)
* mon: allow adjustment of cephfs max file size via 'ceph mds set max_file_size' (Sage Weil)
* mon: allow debug quorum_{enter,exit} commands via admin socket
* mon: 'ceph osd pg-temp ...' and primary-temp commands (Ilya Dryomov)
* mon: change mds allow_new_snaps syntax to be more consistent (Sage Weil)
* mon: clean up initial crush rule creation (Loic Dachary)
* mon: collect misc metadata about osd (os, kernel, etc.), new 'osd metadata' command (Sage Weil)
* mon: do not create erasure rules by default (Sage Weil)
* mon: do not generate spurious MDSMaps in certain cases (Sage Weil)
* mon: do not use keyring if auth = none (Loic Dachary)
* mon: fix peer feature checks (Sage Weil)
* mon: fix pg_temp leaks (Joao Eduardo Luis)
* mon: fix pool count in 'ceph -s' output (Sage Weil)
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
* mon: require 'x' mon caps for auth operations (Joao Luis)
* mon: shutdown when removed from mon cluster (Joao Luis)
* mon: take 'osd pool set ...' value as an int, not string (Joao Eduardo Luis)
* mon: track osd features in OSDMap (Joao Luis, David Zafman)
* mon: trim MDSMaps (Joao Eduardo Luis)
* mon: warn if crush has non-optimal tunables (Sage Weil)
* mount.ceph: add -n for autofs support (Steve Stock)
* msgr: fix locking bug in authentication (Josh Durgin)
* msgr: fix messenger restart race (Xihui He)
* msgr: improve connection error detection between clients and monitors (Greg Farnum, Sage Weil)
* osd: add/fix CPU feature detection for jerasure (Loic Dachary)
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
* osd: fix bug in journal replay/restart (Sage Weil)
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
* osd: improved scrub checks on clones (Sage Weil, Sam Just)
* osd: improve locking in fd lookup cache (Samuel Just, Greg Farnum)
* osd: include more info in pg query result (Sage Weil)
* osd, librados: fix full cluster handling (Josh Durgin)
* osd: many erasure fixes (Sam Just)
* osd: many many many bug fixes with cache tiering (Samuel Just)
* osd: move to jerasure2 library (Loic Dachary)
* osd: new 'chassis' type in default crush hierarchy (Sage Weil)
* osd: new keyvaluestore-dev backend based on leveldb (Haomai Wang)
* osd: new OSDMap encoding (Greg Farnum)
* osd: new tests for erasure pools (David Zafman)
* osd: preliminary cache pool support (no snaps) (Greg Farnum, Sage Weil)
* osd: reduce scrub lock contention (Guang Yang)
* osd: requery unfound on stray notify (#6909) (Samuel Just)
* osd: some PGBackend infrastructure (Samuel Just)
* osd: support for new 'memstore' (memory-backed) backend (Sage Weil)
* osd: track erasure compatibility (David Zafman)
* osd: track omap and hit_set objects in pg stats (Samuel Just)
* osd: warn if agent cannot enable due to invalid (post-split) stats (Sage Weil)
* rados: add 'crush location', smart replica selection/balancing (Sage Weil)
* rados bench: track metadata for multiple runs separately (Guang Yang)
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
* rgw: allow use of an erasure data pool (Yehuda Sadeh)
* rgw: convert bucket info to new format on demand (Yehuda Sadeh)
* rgw: fixed subuser modify (Yehuda Sadeh)
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
* rpm: fix redhat-lsb dependency (Sage Weil, Alfredo Deza)
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

- The radosgw init script (sysvinit) how requires that the 'host = ...' line in
  ceph.conf, if present, match the short hostname (the output of 'hostname -s'),
  not the fully qualified hostname or the (occasionally non-short) output of
  'hostname'.  Failure to adjust this when upgrading from emperor or dumpling
  may prevent the radosgw daemon from starting.


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


v0.67.12 "Dumpling" (draft)
===========================

This stable update for Dumpling fixes a few longstanding issues with
backfill in the OSD that can lead to stalled IOs.  There is also a fix
for memory utilization for reads in librbd when caching is enabled,
and then several other small fixes across the rest of the system.

Dumpling users who have encountered IO stalls during backfill and who
do not expect to upgrade to Firefly soon should upgrade.  Everyone
else should upgrade to Firefly already.  This is likely to be the last stable
release for the 0.67.x Dumpling series.


Notable Changes
---------------

* buffer: fix buffer rebuild alignment corner case (#6614 #6003 Loic Dachary, Samuel Just)
* ceph-disk: reprobe partitions after zap (#9665 #9721 Loic Dachary)
* ceph-disk: use partx instead of partprobe when appropriate (Loic Dachary)
* common: add $cctid meta variable (#6228 Adam Crume)
* crush: fix get_full_location_ordered (Sage Weil)
* crush: pick ruleset id that matches rule_id (#9675 Xiaoxi Chen)
* libcephfs: fix tid wrap bug (#9869 Greg Farnum)
* libcephfs: get osd location on -1 should return EINVAL (Sage Weil)
* librados: fix race condition with C API and op timeouts (#9582 Sage Weil)
* librbd: constrain max number of in-flight read requests (#9854 Jason Dillaman)
* librbd: enforce cache size on read requests (Jason Dillaman)
* librbd: fix invalid close in image open failure path (#10030 Jason Dillaman)
* librbd: fix read hang on sparse files (Jason Dillaman)
* librbd: gracefully handle deleted/renamed pools (#10270 #10122 Jason Dillaman)
* librbd: protect list_children from invalid child pool ioctxs (#10123 Jason Dillaman)
* mds: fix ctime updates from clients without dirty caps (#9514 Greg Farnum)
* mds: fix rare NULL dereference in cap update path (Greg Farnum)
* mds: fix assertion caused by system clock backwards (#11053 Yan, Zheng)
* mds: store backtrace on straydir (Yan, Zheng)
* osd: fix journal committed_thru update after replay (#6756 Samuel Just)
* osd: fix memory leak, busy loop on snap trim (#9113 Samuel Just)
* osd: fix misc peering, recovery bugs (#10168 Samuel Just)
* osd: fix purged_snap field on backfill start (#9487 Sage Weil, Samuel Just)
* osd: handle no-op write with snapshot corner case (#10262 Sage Weil, Loic Dachary)
* osd: respect RWORDERED rados flag (Sage Weil)
* osd: several backfill fixes and refactors (Samuel Just, David Zafman)
* rgw: send http status reason explicitly in fastcgi (Yehuda Sadeh)

v0.67.11 "Dumpling"
===================

This stable update for Dumpling fixes several important bugs that
affect a small set of users.

We recommend that all Dumpling users upgrade at their convenience.  If
none of these issues are affecting your deployment there is no
urgency.


Notable Changes
---------------

* common: fix sending dup cluster log items (#9080 Sage Weil)
* doc: several doc updates (Alfredo Deza)
* libcephfs-java: fix build against older JNI headesr (Greg Farnum)
* librados: fix crash in op timeout path (#9362 Matthias Kiefer, Sage Weil)
* librbd: fix crash using clone of flattened image (#8845 Josh Durgin)
* librbd: fix error path cleanup when failing to open image (#8912 Josh Durgin)
* mon: fix crash when adjusting pg_num before any OSDs are added (#9052 Sage Weil)
* mon: reduce log noise from paxos (Aanchal Agrawal, Sage Weil)
* osd: allow scrub and snap trim thread pool IO priority to be adjusted (Sage Weil)
* osd: fix mount/remount sync race (#9144 Sage Weil)


v0.67.10 "Dumpling"
===================

This stable update release for Dumpling includes primarily fixes for
RGW, including several issues with bucket listings and a potential
data corruption problem when multiple multi-part uploads race.  There is also
some throttling capability added in the OSD for scrub that can mitigate the
performance impact on production clusters.

We recommend that all Dumpling users upgrade at their convenience.

Notable Changes
---------------

* ceph-disk: partprobe befoere settle, fixing dm-crypt (#6966, Eric Eastman)
* librbd: add invalidate cache interface (Josh Durgin)
* librbd: close image if remove_child fails (Ilya Dryomov)
* librbd: fix potential null pointer dereference (Danny Al-Gaaf)
* librbd: improve writeback checks, performance (Haomai Wang)
* librbd: skip zeroes when copying image (#6257, Josh Durgin)
* mon: fix rule(set) check on 'ceph pool set ... crush_ruleset ...' (#8599, John Spray)
* mon: shut down if mon is removed from cluster (#6789, Joao Eduardo Luis)
* osd: fix filestore perf reports to mon (Sage Weil)
* osd: force any new or updated xattr into leveldb if E2BIG from XFS (#7779, Sage Weil)
* osd: lock snapdir object during write to fix race with backfill (Samuel Just)
* osd: option sleep during scrub (Sage Weil)
* osd: set io priority on scrub and snap trim threads (Sage Weil)
* osd: 'status' admin socket command (Sage Weil)
* rbd: tolerate missing NULL terminator on block_name_prefix (#7577, Dan Mick)
* rgw: calculate user manifest (#8169, Yehuda Sadeh)
* rgw: fix abort on chunk read error, avoid using extra memory (#8289, Yehuda Sadeh)
* rgw: fix buffer overflow on bucket instance id (#8608, Yehuda Sadeh)
* rgw: fix crash in swift CORS preflight request (#8586, Yehuda Sadeh)
* rgw: fix implicit removal of old objects on object creation (#8972, Patrycja Szablowska, Yehuda Sadeh)
* rgw: fix MaxKeys in bucket listing (Yehuda Sadeh)
* rgw: fix race with multiple updates to a single multipart object (#8269, Yehuda Sadeh)
* rgw: improve bucket listing with delimiter (Yehuda Sadeh)
* rgw: include NextMarker in bucket listing (#8858, Yehuda Sadeh)
* rgw: return error early on non-existent bucket (#7064, Yehuda Sadeh)
* rgw: set truncation flag correctly in bucket listing (Yehuda Sadeh)
* sysvinit: continue starting daemons after pre-mount error (#8554, Sage Weil)

For more detailed information, see :download:`the complete changelog <changelog/v0.67.10.txt>`.


v0.67.9 "Dumpling"
==================

This Dumpling point release fixes several minor bugs. The most
prevalent in the field is one that occasionally prevents OSDs from
starting on recently created clusters.

We recommend that all Dumpling users upgrade at their convenience.

Notable Changes
---------------

* ceph-fuse, libcephfs: client admin socket command to kick and inspect MDS sessions (#8021, Zheng Yan)
* monclient: fix failure detection during mon handshake (#8278, Sage Weil)
* mon: set tid on no-op PGStatsAck messages (#8280, Sage Weil)
* msgr: fix a rare bug with connection negotiation between OSDs (Guang Yang)
* osd: allow snap trim throttling with simple delay (#6278, Sage Weil)
* osd: check for splitting when processing recover/backfill reservations (#6565, Samuel Just)
* osd: fix backfill position tracking (#8162, Samuel Just)
* osd: fix bug in backfill stats (Samuel Just)
* osd: fix bug preventing OSD startup for infant clusters (#8162, Greg Farnum)
* osd: fix rare PG resurrection race causing an incomplete PG (#7740, Samuel Just)
* osd: only complete replicas count toward min_size (#7805, Samuel Just)
* rgw: allow setting ACLs with empty owner (#6892, Yehuda Sadeh)
* rgw: send user manifest header field (#8170, Yehuda Sadeh)

For more detailed information, see :download:`the complete changelog <changelog/v0.67.9.txt>`.


v0.67.8 "Dumpling"
==================

This Dumpling point release fixes several non-critical issues since
v0.67.7.  The most notable bug fixes are an auth fix in librbd
(observed as an occasional crash from KVM), an improvement in the
network failure detection with the monitor, and several hard to hit
OSD crashes or hangs.

We recommend that all users upgrade at their convenience.

Upgrading
---------

* The 'rbd ls' function now returns success and returns an empty when a pool
  does not store any rbd images.  Previously it would return an ENOENT error.

* Ceph will now issue a health warning if the 'mon osd down out
  interval' config option is set to zero.  This warning can be
  disabled by adding 'mon warn on osd down out interval zero = false'
  to ceph.conf.

Notable Changes
---------------

* all: improve keepalive detection of failed monitor connections (#7888, Sage Weil)
* ceph-fuse, libcephfs: pin inodes during readahead, fixing rare crash (#7867, Sage Weil)
* librbd: make cache writeback a bit less aggressive (Sage Weil)
* librbd: make symlink for qemu to detect librbd in RPM (#7293, Josh Durgin)
* mon: allow 'hashpspool' pool flag to be set and unset (Loic Dachary)
* mon: commit paxos state only after entire quorum acks, fixing rare race where prior round state is readable (#7736, Sage Weil)
* mon: make elections and timeouts a bit more robust (#7212, Sage Weil)
* mon: prevent extreme pool split operations (Greg Farnum)
* mon: wait for quorum for get_version requests to close rare pool creation race (#7997, Sage Weil)
* mon: warn on 'mon osd down out interval = 0' (#7784, Joao Luis)
* msgr: fix byte-order for auth challenge, fixing auth errors on big-endian clients (#7977, Dan Mick)
* msgr: fix occasional crash in authentication code (usually triggered by librbd) (#6840, Josh Durgin)
* msgr: fix rebind() race (#6992, Xihui He)
* osd: avoid timeouts during slow PG deletion (#6528, Samuel Just)
* osd: fix bug in pool listing during recovery (#6633, Samuel Just)
* osd: fix queue limits, fixing recovery stalls (#7706, Samuel Just)
* osd: fix rare peering crashes (#6722, #6910, Samuel Just)
* osd: fix rare recovery hang (#6681, Samuel Just)
* osd: improve error handling on journal errors (#7738, Sage Weil)
* osd: reduce load on the monitor from OSDMap subscriptions (Greg Farnum)
* osd: rery GetLog on peer osd startup, fixing some rare peering stalls (#6909, Samuel Just)
* osd: reset journal state on remount to fix occasional crash on OSD startup (#8019, Sage Weil)
* osd: share maps with peers more aggressively (Greg Farnum)
* rbd: make it harder to delete an rbd image that is currently in use (#7076, Ilya Drymov)
* rgw: deny writes to secondary zone by non-system users (#6678, Yehuda Sadeh)
* rgw: do'nt log system requests in usage log (#6889, Yehuda Sadeh)
* rgw: fix bucket recreation (#6951, Yehuda Sadeh)
* rgw: fix Swift range response (#7099, Julien Calvet, Yehuda Sadeh)
* rgw: fix URL escaping (#8202, Yehuda Sadeh)
* rgw: fix whitespace trimming in http headers (#7543, Yehuda Sadeh)
* rgw: make multi-object deletion idempotent (#7346, Yehuda Sadeh)

For more detailed information, see :download:`the complete changelog <changelog/v0.67.8.txt>`.

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


.. _Dumpling upgrade:

Upgrade Sequencing
------------------

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




