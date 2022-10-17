========
Dumpling
========

Dumpling is the 4th stable release of Ceph.  It is named after the
dumpling squid (Euprymna tasmanica).

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

For more detailed information, see :download:`the complete changelog <../changelog/v0.67.10.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.67.9.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.67.8.txt>`.

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

For more detailed information, see :download:`the complete changelog <../changelog/v0.67.7.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.67.6.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.67.5.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.67.4.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.67.3.txt>`.


v0.67.2 "Dumpling"
==================

This is an important point release for Dumpling.  Most notably, it
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

For more detailed information, see :download:`the complete changelog <../changelog/v0.67.2.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.67.1.txt>`.

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


.. _dumpling-upgrade:

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
  in order to accommodate the new write back throttle system.  On
  Ubuntu, upstart now sets the fd limit to 32k.  On other platforms,
  the sysvinit script will set it to 32k by default (still
  overridable via max_open_files).  If this field has been customized
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
  the cluster or creating new pools.  Note that the presence of any
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
