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

For more detailed information, see :download:`the complete changelog <../changelog/v0.56.7.txt>`.


v0.56.6 "bobtail"
=================

Notable changes
---------------

* rgw: fix garbage collection
* rpm: fix package dependencies

For more detailed information, see :download:`the complete changelog <../changelog/v0.56.6.txt>`.


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
* librbd: new async flush method to resolve qemu hangs (requires QEMU update as well)
* librbd: a few fixes to flatten
* ceph-disk: support for dm-crypt
* ceph-disk: many backports to allow bobtail deployments with ceph-deploy, chef
* sysvinit: do not stop starting daemons on first failure
* udev: fixed rules for redhat-based distros
* build fixes for raring

For more detailed information, see :download:`the complete changelog <../changelog/v0.56.5.txt>`.

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

For more detailed information, see :download:`the complete changelog <../changelog/v0.56.4.txt>`.

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

For more detailed information, see :download:`the complete changelog <../changelog/v0.56.3.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.56.2.txt>`.


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

For more detailed information, see :download:`the complete changelog <../changelog/v0.56.1.txt>`.

v0.56 "bobtail"
===============

Bobtail is the second stable release of Ceph, named in honor of the
`Bobtail Squid`: https://en.wikipedia.org/wiki/Bobtail_squid.

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
  disabled, and only then upgrading to the latest version::

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
