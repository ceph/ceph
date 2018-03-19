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

For more detailed information, see :download:`the complete changelog <../changelog/v0.61.9.txt>`.

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

For more detailed information, see :download:`the complete changelog <../changelog/v0.61.8.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.61.7.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.61.6.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.61.5.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.61.4.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.61.3.txt>`.


v0.61.2 "Cuttlefish"
====================

This release disables a monitor debug log that consumes disk space and
fixes a bug when upgrade some monitors from bobtail to cuttlefish.

Notable Changes
---------------

* mon: fix conversion of stores with duplicated GV values
* mon: disable 'mon debug dump transactions' by default

For more detailed information, see :download:`the complete changelog <../changelog/v0.61.2.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.61.1.txt>`.

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
