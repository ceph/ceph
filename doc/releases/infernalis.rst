v9.2.1 Infernalis
=================

This Infernalis point release fixes several packagins and init script
issues, enables the librbd objectmap feature by default, a few librbd
bugs, and a range of miscellaneous bug fixes across the system.

We recommend that all infernalis v9.2.0 users upgrade.

For more detailed information, see :download:`the complete changelog <../changelog/v9.2.1.txt>`.

Upgrading
---------

* Some symbols wrongly exposed by the C++ interface for librados in
  v9.1.0 and v9.2.0 were removed.  If you compiled your own
  application against librados shipped with these releases, it is very
  likely referencing these removed symbols. So you will need to
  recompile it.


Notable Changes
---------------
* build/ops: Ceph daemon failed to start, because the service name was already used. (`issue#13474 <http://tracker.ceph.com/issues/13474>`_, `pr#6833 <http://github.com/ceph/ceph/pull/6833>`_, Chuanhong Wang)
* build/ops: ceph upstart script rbdmap.conf incorrectly processes parameters (`issue#13214 <http://tracker.ceph.com/issues/13214>`_, `pr#6396 <http://github.com/ceph/ceph/pull/6396>`_, Sage Weil)
* build/ops: libunwind package missing on CentOS 7 (`issue#13997 <http://tracker.ceph.com/issues/13997>`_, `pr#6845 <http://github.com/ceph/ceph/pull/6845>`_, Loic Dachary)
* build/ops: rbd-replay-* moved from ceph-test-dbg to ceph-common-dbg as well (`issue#13785 <http://tracker.ceph.com/issues/13785>`_, `pr#6628 <http://github.com/ceph/ceph/pull/6628>`_, Loic Dachary)
* build/ops: systemd/ceph-disk@.service assumes /bin/flock (`issue#13975 <http://tracker.ceph.com/issues/13975>`_, `pr#6852 <http://github.com/ceph/ceph/pull/6852>`_, Loic Dachary)
* build/ops: systemd: no rbdmap systemd unit file (`issue#13374 <http://tracker.ceph.com/issues/13374>`_, `pr#6500 <http://github.com/ceph/ceph/pull/6500>`_, Boris Ranto)
* common: auth/cephx: large amounts of log are produced by osd (`issue#13610 <http://tracker.ceph.com/issues/13610>`_, `pr#6836 <http://github.com/ceph/ceph/pull/6836>`_, Qiankun Zheng)
* common: log: Log.cc: Assign LOG_DEBUG priority to syslog calls (`issue#13993 <http://tracker.ceph.com/issues/13993>`_, `pr#6993 <http://github.com/ceph/ceph/pull/6993>`_, Brad Hubbard)
* crush: crash if we see CRUSH_ITEM_NONE in early rule step (`issue#13477 <http://tracker.ceph.com/issues/13477>`_, `pr#6626 <http://github.com/ceph/ceph/pull/6626>`_, Sage Weil)
* fs: Ceph file system is not freeing space (`issue#13777 <http://tracker.ceph.com/issues/13777>`_, `pr#7431 <http://github.com/ceph/ceph/pull/7431>`_, Yan, Zheng, John Spray)
* fs: Ceph-fuse won't start correctly when the option log_max_new in ceph.conf set to zero (`issue#13443 <http://tracker.ceph.com/issues/13443>`_, `pr#6395 <http://github.com/ceph/ceph/pull/6395>`_, Wenjun Huang)
* fs: Segmentation fault accessing file using fuse mount (`issue#13714 <http://tracker.ceph.com/issues/13714>`_, `pr#6853 <http://github.com/ceph/ceph/pull/6853>`_, Yan, Zheng)
* librbd: Avoid re-writing old-format image header on resize (`issue#13674 <http://tracker.ceph.com/issues/13674>`_, `pr#6630 <http://github.com/ceph/ceph/pull/6630>`_, Jason Dillaman)
* librbd: ImageWatcher shouldn't block the notification thread (`issue#14373 <http://tracker.ceph.com/issues/14373>`_, `pr#7406 <http://github.com/ceph/ceph/pull/7406>`_, Jason Dillaman)
* librbd: QEMU hangs after creating snapshot and stopping VM (`issue#13726 <http://tracker.ceph.com/issues/13726>`_, `pr#6632 <http://github.com/ceph/ceph/pull/6632>`_, Jason Dillaman)
* librbd: Verify self-managed snapshot functionality on image create (`issue#13633 <http://tracker.ceph.com/issues/13633>`_, `pr#7080 <http://github.com/ceph/ceph/pull/7080>`_, Jason Dillaman)
* librbd: [ FAILED ] TestLibRBD.SnapRemoveViaLockOwner (`issue#14164 <http://tracker.ceph.com/issues/14164>`_, `pr#7079 <http://github.com/ceph/ceph/pull/7079>`_, Jason Dillaman)
* librbd: enable feature objectmap (`issue#13558 <http://tracker.ceph.com/issues/13558>`_, `pr#6477 <http://github.com/ceph/ceph/pull/6477>`_, xinxin shu)
* librbd: fix merge-diff for >2GB diff-files (`issue#14030 <http://tracker.ceph.com/issues/14030>`_, `pr#6981 <http://github.com/ceph/ceph/pull/6981>`_, Jason Dillaman)
* librbd: flattening an rbd image with active IO can lead to hang (`issue#14092 <http://tracker.ceph.com/issues/14092>`_, `issue#14483 <http://tracker.ceph.com/issues/14483>`_, `pr#7484 <http://github.com/ceph/ceph/pull/7484>`_, Jason Dillaman)
* mds: fix client capabilities during reconnect (client.XXXX isn't responding to mclientcaps warning) (`issue#11482 <http://tracker.ceph.com/issues/11482>`_, `pr#6752 <http://github.com/ceph/ceph/pull/6752>`_, Yan, Zheng)
* mon: Ceph Pools' MAX AVAIL is 0 if some OSDs' weight is 0 (`issue#13840 <http://tracker.ceph.com/issues/13840>`_, `pr#6907 <http://github.com/ceph/ceph/pull/6907>`_, Chengyuan Li)
* mon: should not set isvalid = true when cephx_verify_authorizer retur... (`issue#13525 <http://tracker.ceph.com/issues/13525>`_, `pr#6392 <http://github.com/ceph/ceph/pull/6392>`_, Ruifeng Yang)
* objecter: pool op callback may hang forever. (`issue#13642 <http://tracker.ceph.com/issues/13642>`_, `pr#6627 <http://github.com/ceph/ceph/pull/6627>`_, xie xingguo)
* objecter: potential null pointer access when do pool_snap_list. (`issue#13639 <http://tracker.ceph.com/issues/13639>`_, `pr#6840 <http://github.com/ceph/ceph/pull/6840>`_, xie xingguo)
* osd: FileStore: potential memory leak if getattrs fails. (`issue#13597 <http://tracker.ceph.com/issues/13597>`_, `pr#6846 <http://github.com/ceph/ceph/pull/6846>`_, xie xingguo)
* osd: OSD::build_past_intervals_parallel() shall reset primary and up_primary when begin a new past_interval. (`issue#13471 <http://tracker.ceph.com/issues/13471>`_, `pr#6397 <http://github.com/ceph/ceph/pull/6397>`_, xiexingguo)
* osd: call on_new_interval on newly split child PG (`issue#13962 <http://tracker.ceph.com/issues/13962>`_, `pr#6849 <http://github.com/ceph/ceph/pull/6849>`_, Sage Weil)
* osd: ceph-disk list fails on /dev/cciss!c0d0 (`issue#13970 <http://tracker.ceph.com/issues/13970>`_, `issue#14230 <http://tracker.ceph.com/issues/14230>`_, `pr#6880 <http://github.com/ceph/ceph/pull/6880>`_, Loic Dachary)
* osd: ceph-disk: use blkid instead of sgdisk -i (`issue#14080 <http://tracker.ceph.com/issues/14080>`_, `pr#7001 <http://github.com/ceph/ceph/pull/7001>`_, Loic Dachary, Ilya Dryomov)
* osd: fix race condition during send_failures (`issue#13821 <http://tracker.ceph.com/issues/13821>`_, `pr#6694 <http://github.com/ceph/ceph/pull/6694>`_, Sage Weil)
* osd: osd/PG.cc: 288: FAILED assert(info.last_epoch_started >= info.history.last_epoch_started) (`issue#14015 <http://tracker.ceph.com/issues/14015>`_, `pr#6851 <http://github.com/ceph/ceph/pull/6851>`_, David Zafman)
* osd: pgs stuck inconsistent after infernalis upgrade (`issue#13862 <http://tracker.ceph.com/issues/13862>`_, `pr#7421 <http://github.com/ceph/ceph/pull/7421>`_, David Zafman)
* rbd: TaskFinisher::cancel should remove event from SafeTimer (`issue#14476 <http://tracker.ceph.com/issues/14476>`_, `pr#7426 <http://github.com/ceph/ceph/pull/7426>`_, Douglas Fuller)
* rbd: cls_rbd: object_map_save should enable checksums (`issue#14280 <http://tracker.ceph.com/issues/14280>`_, `pr#7428 <http://github.com/ceph/ceph/pull/7428>`_, Douglas Fuller)
* rbd: misdirected op in rbd balance-reads test (`issue#13491 <http://tracker.ceph.com/issues/13491>`_, `pr#6629 <http://github.com/ceph/ceph/pull/6629>`_, Jason Dillaman)
* rbd: pure virtual method called (`issue#13636 <http://tracker.ceph.com/issues/13636>`_, `pr#6633 <http://github.com/ceph/ceph/pull/6633>`_, Jason Dillaman)
* rbd: rbd clone issue (`issue#13553 <http://tracker.ceph.com/issues/13553>`_, `pr#6474 <http://github.com/ceph/ceph/pull/6474>`_, xinxin shu)
* rbd: rbd-replay does not check for EOF and goes to endless loop (`issue#14452 <http://tracker.ceph.com/issues/14452>`_, `pr#7427 <http://github.com/ceph/ceph/pull/7427>`_, Mykola Golub)
* rbd: unknown argument --quiet in udevadm settle (`issue#13560 <http://tracker.ceph.com/issues/13560>`_, `pr#6634 <http://github.com/ceph/ceph/pull/6634>`_, Jason Dillaman)
* rgw: init script reload doesn't work on EL7 (`issue#13709 <http://tracker.ceph.com/issues/13709>`_, `pr#6650 <http://github.com/ceph/ceph/pull/6650>`_, Hervé Rousseau)
* rgw: radosgw-admin --help doesn't show the orphans find command (`issue#14516 <http://tracker.ceph.com/issues/14516>`_, `pr#7543 <http://github.com/ceph/ceph/pull/7543>`_, Yehuda Sadeh)
* tests: ceph-disk workunit uses configobj (`issue#14004 <http://tracker.ceph.com/issues/14004>`_, `pr#6828 <http://github.com/ceph/ceph/pull/6828>`_, Loic Dachary)
* tests: fsx failed to compile (`issue#14384 <http://tracker.ceph.com/issues/14384>`_, `pr#7429 <http://github.com/ceph/ceph/pull/7429>`_, Greg Farnum)
* tests: notification slave needs to wait for master (`issue#13810 <http://tracker.ceph.com/issues/13810>`_, `pr#7225 <http://github.com/ceph/ceph/pull/7225>`_, Jason Dillaman)
* tests: rebuild exclusive lock test should acquire exclusive lock (`issue#14121 <http://tracker.ceph.com/issues/14121>`_, `pr#7038 <http://github.com/ceph/ceph/pull/7038>`_, Jason Dillaman)
* tests: testprofile must be removed before it is re-created (`issue#13664 <http://tracker.ceph.com/issues/13664>`_, `pr#6449 <http://github.com/ceph/ceph/pull/6449>`_, Loic Dachary)
* tests: verify it is possible to reuse an OSD id (`issue#13988 <http://tracker.ceph.com/issues/13988>`_, `pr#6882 <http://github.com/ceph/ceph/pull/6882>`_, Loic Dachary)

v9.2.0 Infernalis
=================

This major release will be the foundation for the next stable series.
There have been some major changes since v0.94.x Hammer, and the
upgrade process is non-trivial.  Please read these release notes carefully.

Major Changes from Hammer
-------------------------

- *General*:

  * Ceph daemons are now managed via systemd (with the exception of
    Ubuntu Trusty, which still uses upstart).
  * Ceph daemons run as 'ceph' user instead root.
  * On Red Hat distros, there is also an SELinux policy.

- *RADOS*:

  * The RADOS cache tier can now proxy write operations to the base
    tier, allowing writes to be handled without forcing migration of
    an object into the cache.
  * The SHEC erasure coding support is no longer flagged as
    experimental. SHEC trades some additional storage space for faster
    repair.
  * There is now a unified queue (and thus prioritization) of client
    IO, recovery, scrubbing, and snapshot trimming.
  * There have been many improvements to low-level repair tooling
    (ceph-objectstore-tool).
  * The internal ObjectStore API has been significantly cleaned up in order
    to faciliate new storage backends like NewStore.

- *RGW*:

  * The Swift API now supports object expiration.
  * There are many Swift API compatibility improvements.

- *RBD*:

  * The ``rbd du`` command shows actual usage (quickly, when
    object-map is enabled).
  * The object-map feature has seen many stability improvements.
  * Object-map and exclusive-lock features can be enabled or disabled
    dynamically.
  * You can now store user metadata and set persistent librbd options
    associated with individual images.
  * The new deep-flatten features allows flattening of a clone and all
    of its snapshots.  (Previously snapshots could not be flattened.)
  * The export-diff command command is now faster (it uses aio).  There is also
    a new fast-diff feature.
  * The --size argument can be specified with a suffix for units
    (e.g., ``--size 64G``).
  * There is a new ``rbd status`` command that, for now, shows who has
    the image open/mapped.

- *CephFS*:

  * You can now rename snapshots.
  * There have been ongoing improvements around administration, diagnostics,
    and the check and repair tools.
  * The caching and revocation of client cache state due to unused
    inodes has been dramatically improved.
  * The ceph-fuse client behaves better on 32-bit hosts.

Distro compatibility
--------------------

We have decided to drop support for many older distributions so that we can
move to a newer compiler toolchain (e.g., C++11).  Although it is still possible
to build Ceph on older distributions by installing backported development tools,
we are not building and publishing release packages for ceph.com.

We now build packages for:

* CentOS 7 or later.  We have dropped support for CentOS 6 (and other
  RHEL 6 derivatives, like Scientific Linux 6).
* Debian Jessie 8.x or later.  Debian Wheezy 7.x's g++ has incomplete
  support for C++11 (and no systemd).
* Ubuntu Trusty 14.04 or later.  Ubuntu Precise 12.04 is no longer
  supported.
* Fedora 22 or later.

Upgrading from Firefly
----------------------

Upgrading directly from Firefly v0.80.z is not recommended.  It is
possible to do a direct upgrade, but not without downtime.  We
recommend that clusters are first upgraded to Hammer v0.94.4 or a
later v0.94.z release; only then is it possible to upgrade to
Infernalis 9.2.z for an online upgrade (see below).

To do an offline upgrade directly from Firefly, all Firefly OSDs must
be stopped and marked down before any Infernalis OSDs will be allowed
to start up.  This fencing is enforced by the Infernalis monitor, so
use an upgrade procedure like:

#. Upgrade Ceph on monitor hosts
#. Restart all ceph-mon daemons
#. Upgrade Ceph on all OSD hosts
#. Stop all ceph-osd daemons
#. Mark all OSDs down with something like::

     ceph osd down `seq 0 1000`

#. Start all ceph-osd daemons
#. Upgrade and restart remaining daemons (ceph-mds, radosgw)

Upgrading from Hammer
---------------------

* All cluster nodes must first upgrade to Hammer v0.94.4 or a later v0.94.z release; only
  then is it possible to upgrade to Infernalis 9.2.z.

* For all distributions that support systemd (CentOS 7, Fedora, Debian
  Jessie 8.x, OpenSUSE), ceph daemons are now managed using native systemd
  files instead of the legacy sysvinit scripts.  For example::

    systemctl start ceph.target       # start all daemons
    systemctl status ceph-osd@12      # check status of osd.12

  The main notable distro that is *not* yet using systemd is Ubuntu trusty
  14.04.  (The next Ubuntu LTS, 16.04, will use systemd instead of upstart.)

* Ceph daemons now run as user and group ``ceph`` by default.  The
  ceph user has a static UID assigned by Fedora and Debian (also used
  by derivative distributions like RHEL/CentOS and Ubuntu).  On SUSE
  the ceph user will currently get a dynamically assigned UID when the
  user is created.

  If your systems already have a ceph user, upgrading the package will cause
  problems.  We suggest you first remove or rename the existing 'ceph' user
  and 'ceph' group before upgrading.

  When upgrading, administrators have two options:

   #. Add the following line to ``ceph.conf`` on all hosts::

        setuser match path = /var/lib/ceph/$type/$cluster-$id

      This will make the Ceph daemons run as root (i.e., not drop
      privileges and switch to user ceph) if the daemon's data
      directory is still owned by root.  Newly deployed daemons will
      be created with data owned by user ceph and will run with
      reduced privileges, but upgraded daemons will continue to run as
      root.

   #. Fix the data ownership during the upgrade.  This is the
      preferred option, but it is more work and can be very time
      consuming.  The process for each host is to:

   #. Upgrade the ceph package.  This creates the ceph user and group.  For
      example::

        ceph-deploy install --stable infernalis HOST

      #. Stop the daemon(s)::

           service ceph stop           # fedora, centos, rhel, debian
           stop ceph-all               # ubuntu

      #. Fix the ownership::

           chown -R ceph:ceph /var/lib/ceph
           chown -R ceph:ceph /var/log/ceph

      #. Restart the daemon(s)::

           start ceph-all                # ubuntu
           systemctl start ceph.target   # debian, centos, fedora, rhel

      Alternatively, the same process can be done with a single daemon
      type, for example by stopping only monitors and chowning only
      ``/var/lib/ceph/mon``.

* The on-disk format for the experimental KeyValueStore OSD backend has
  changed.  You will need to remove any OSDs using that backend before you
  upgrade any test clusters that use it.

* When a pool quota is reached, librados operations now block indefinitely,
  the same way they do when the cluster fills up.  (Previously they would return
  -ENOSPC).  By default, a full cluster or pool will now block.  If your
  librados application can handle ENOSPC or EDQUOT errors gracefully, you can
  get error returns instead by using the new librados OPERATION_FULL_TRY flag.

* The return code for librbd's rbd_aio_read and Image::aio_read API methods no
  longer returns the number of bytes read upon success.  Instead, it returns 0
  upon success and a negative value upon failure.

* 'ceph scrub', 'ceph compact' and 'ceph sync force are now DEPRECATED.  Users
  should instead use 'ceph mon scrub', 'ceph mon compact' and
  'ceph mon sync force'.

* 'ceph mon_metadata' should now be used as 'ceph mon metadata'. There is no
  need to deprecate this command (same major release since it was first
  introduced).

* The `--dump-json` option of "osdmaptool" is replaced by `--dump json`.

* The commands of "pg ls-by-{pool,primary,osd}" and "pg ls" now take "recovering"
  instead of "recovery", to include the recovering pgs in the listed pgs.

Notable Changes since Hammer
----------------------------

* aarch64: add optimized version of crc32c (Yazen Ghannam, Steve Capper)
* auth: cache/reuse crypto lib key objects, optimize msg signature check (Sage Weil)
* auth: reinit NSS after fork() (#11128 Yan, Zheng)
* autotools: fix out of tree build (Krxysztof Kosinski)
* autotools: improve make check output (Loic Dachary)
* buffer: add invalidate_crc() (Piotr Dalek)
* buffer: fix zero bug (#12252 Haomai Wang)
* buffer: some cleanup (Michal Jarzabek)
* build: allow tcmalloc-minimal (Thorsten Behrens)
* build: C++11 now supported
* build: cmake: fix nss linking (Danny Al-Gaaf)
* build: cmake: misc fixes (Orit Wasserman, Casey Bodley)
* build: disable LTTNG by default (#11333 Josh Durgin)
* build: do not build ceph-dencoder with tcmalloc (#10691 Boris Ranto)
* build: fix junit detection on Fedora 22 (Ira Cooper)
* build: fix pg ref disabling (William A. Kennington III)
* build: fix ppc build (James Page)
* build: install-deps: misc fixes (Loic Dachary)
* build: install-deps.sh improvements (Loic Dachary)
* build: install-deps: support OpenSUSE (Loic Dachary)
* build: make_dist_tarball.sh (Sage Weil)
* build: many cmake improvements
* build: misc cmake fixes (Matt Benjamin)
* build: misc fixes (Boris Ranto, Ken Dreyer, Owen Synge)
* build: OSX build fixes (Yan, Zheng)
* build: remove rest-bench
* ceph-authtool: fix return code on error (Gerhard Muntingh)
* ceph-detect-init: added Linux Mint (Michal Jarzabek)
* ceph-detect-init: robust init system detection (Owen Synge)
* ceph-disk: ensure 'zap' only operates on a full disk (#11272 Loic Dachary)
* ceph-disk: fix zap sgdisk invocation (Owen Synge, Thorsten Behrens)
* ceph-disk: follow ceph-osd hints when creating journal (#9580 Sage Weil)
* ceph-disk: handle re-using existing partition (#10987 Loic Dachary)
* ceph-disk: improve parted output parsing (#10983 Loic Dachary)
* ceph-disk: install pip > 6.1 (#11952 Loic Dachary)
* ceph-disk: make suppression work for activate-all and activate-journal (Dan van der Ster)
* ceph-disk: many fixes (Loic Dachary, Alfredo Deza)
* ceph-disk: fixes to respect init system (Loic Dachary, Owen Synge)
* ceph-disk: pass --cluster arg on prepare subcommand (Kefu Chai)
* ceph-disk: support for multipath devices (Loic Dachary)
* ceph-disk: support NVMe device partitions (#11612 Ilja Slepnev)
* ceph: fix 'df' units (Zhe Zhang)
* ceph: fix parsing in interactive cli mode (#11279 Kefu Chai)
* cephfs-data-scan: many additions, improvements (John Spray)
* ceph-fuse: do not require successful remount when unmounting (#10982 Greg Farnum)
* ceph-fuse, libcephfs: don't clear COMPLETE when trimming null (Yan, Zheng)
* ceph-fuse, libcephfs: drop inode when rmdir finishes (#11339 Yan, Zheng)
* ceph-fuse,libcephfs: fix uninline (#11356 Yan, Zheng)
* ceph-fuse, libcephfs: hold exclusive caps on dirs we "own" (#11226 Greg Farnum)
* ceph-fuse: mostly behave on 32-bit hosts (Yan, Zheng)
* ceph: improve error output for 'tell' (#11101 Kefu Chai)
* ceph-monstore-tool: fix store-copy (Huangjun)
* ceph: new 'ceph daemonperf' command (John Spray, Mykola Golub)
* ceph-objectstore-tool: many many improvements (David Zafman)
* ceph-objectstore-tool: refactoring and cleanup (John Spray)
* ceph-post-file: misc fixes (Joey McDonald, Sage Weil)
* ceph_test_rados: test pipelined reads (Zhiqiang Wang)
* client: avoid sending unnecessary FLUSHSNAP messages (Yan, Zheng)
* client: exclude setfilelock when calculating oldest tid (Yan, Zheng)
* client: fix error handling in check_pool_perm (John Spray)
* client: fsync waits only for inode's caps to flush (Yan, Zheng)
* client: invalidate kernel dcache when cache size exceeds limits (Yan, Zheng)
* client: make fsync wait for unsafe dir operations (Yan, Zheng)
* client: pin lookup dentry to avoid inode being freed (Yan, Zheng)
* common: add descriptions to perfcounters (Kiseleva Alyona)
* common: add perf counter descriptions (Alyona Kiseleva)
* common: bufferlist performance tuning (Piotr Dalek, Sage Weil)
* common: detect overflow of int config values (#11484 Kefu Chai)
* common: fix bit_vector extent calc (#12611 Jason Dillaman)
* common: fix json parsing of utf8 (#7387 Tim Serong)
* common: fix leak of pthread_mutexattr (#11762 Ketor Meng)
* common: fix LTTNG vs fork issue (Josh Durgin)
* common: fix throttle max change (Henry Chang)
* common: make mutex more efficient
* common: make work queue addition/removal thread safe (#12662 Jason Dillaman)
* common: optracker improvements (Zhiqiang Wang, Jianpeng Ma)
* common: PriorityQueue tests (Kefu Chai)
* common: some async compression infrastructure (Haomai Wang)
* crush: add --check to validate dangling names, max osd id (Kefu Chai)
* crush: cleanup, sync with kernel (Ilya Dryomov)
* crush: fix crash from invalid 'take' argument (#11602 Shiva Rkreddy, Sage Weil)
* crush: fix divide-by-2 in straw2 (#11357 Yann Dupont, Sage Weil)
* crush: fix has_v4_buckets (#11364 Sage Weil)
* crush: fix subtree base weight on adjust_subtree_weight (#11855 Sage Weil)
* crush: respect default replicated ruleset config on map creation (Ilya Dryomov)
* crushtool: fix order of operations, usage (Sage Weil)
* crypto: fix NSS leak (Jason Dillaman)
* crypto: fix unbalanced init/shutdown (#12598 Zheng Yan)
* deb: fix rest-bench-dbg and ceph-test-dbg dependendies (Ken Dreyer)
* debian: minor package reorg (Ken Dreyer)
* deb, rpm: move ceph-objectstore-tool to ceph (Ken Dreyer)
* doc: docuemnt object corpus generation (#11099 Alexis Normand)
* doc: document region hostnames (Robin H. Johnson)
* doc: fix gender neutrality (Alexandre Maragone)
* doc: fix install doc (#10957 Kefu Chai)
* doc: fix sphinx issues (Kefu Chai)
* doc: man page updates (Kefu Chai)
* doc: mds data structure docs (Yan, Zheng)
* doc: misc updates (Fracois Lafont, Ken Dreyer, Kefu Chai, Owen Synge, Gael Fenet-Garde, Loic Dachary, Yannick Atchy-Dalama, Jiaying Ren, Kevin Caradant, Robert Maxime, Nicolas Yong, Germain Chipaux, Arthur Gorjux, Gabriel Sentucq, Clement Lebrun, Jean-Remi Deveaux, Clair Massot, Robin Tang, Thomas Laumondais, Jordan Dorne, Yuan Zhou, Valentin Thomas, Pierre Chaumont, Benjamin Troquereau, Benjamin Sesia, Vikhyat Umrao, Nilamdyuti Goswami, Vartika Rai, Florian Haas, Loic Dachary, Simon Guinot, Andy Allan, Alistair Israel, Ken Dreyer, Robin Rehu, Lee Revell, Florian Marsylle, Thomas Johnson, Bosse Klykken, Travis Rhoden, Ian Kelling)
* doc: swift tempurls (#10184 Abhishek Lekshmanan)
* doc: switch doxygen integration back to breathe (#6115 Kefu Chai)
* doc: update release schedule docs (Loic Dachary)
* erasure-code: cleanup (Kefu Chai)
* erasure-code: improve tests (Loic Dachary)
* erasure-code: shec: fix recovery bugs (Takanori Nakao, Shotaro Kawaguchi)
* erasure-code: update ISA-L to 2.13 (Yuan Zhou)
* gmock: switch to submodule (Danny Al-Gaaf, Loic Dachary)
* hadoop: add terasort test (Noah Watkins)
* init-radosgw: merge with sysv version; fix enumeration (Sage Weil)
* java: fix libcephfs bindings (Noah Watkins)
* libcephfs: add pread, pwrite (Jevon Qiao)
* libcephfs,ceph-fuse: cache cleanup (Zheng Yan)
* libcephfs,ceph-fuse: fix request resend on cap reconnect (#10912 Yan, Zheng)
* librados: add config observer (Alistair Strachan)
* librados: add FULL_TRY and FULL_FORCE flags for dealing with full clusters or pools (Sage Weil)
* librados: add src_fadvise_flags for copy-from (Jianpeng Ma)
* librados: define C++ flags from C constants (Josh Durgin)
* librados: fadvise flags per op (Jianpeng Ma)
* librados: fix last_force_resent handling (#11026 Jianpeng Ma)
* librados: fix memory leak from C_TwoContexts (Xiong Yiliang)
* librados: fix notify completion race (#13114 Sage Weil)
* librados: fix striper when stripe_count = 1 and stripe_unit != object_size (#11120 Yan, Zheng)
* librados, libcephfs: randomize client nonces (Josh Durgin)
* librados: op perf counters (John Spray)
* librados: pybind: fix binary omap values (Robin H. Johnson)
* librados: pybind: fix write() method return code (Javier Guerra)
* librados: respect default_crush_ruleset on pool_create (#11640 Yuan Zhou)
* libradosstriper: fix leak (Danny Al-Gaaf)
* librbd: add const for single-client-only features (Josh Durgin)
* librbd: add deep-flatten operation (Jason Dillaman)
* librbd: add purge_on_error cache behavior (Jianpeng Ma)
* librbd: allow additional metadata to be stored with the image (Haomai Wang)
* librbd: avoid blocking aio API methods (#11056 Jason Dillaman)
* librbd: better handling for dup flatten requests (#11370 Jason Dillaman)
* librbd: cancel in-flight ops on watch error (#11363 Jason Dillaman)
* librbd: default new images to format 2 (#11348 Jason Dillaman)
* librbd: fadvise for copy, export, import (Jianpeng Ma)
* librbd: fast diff implementation that leverages object map (Jason Dillaman)
* librbd: fix fast diff bugs (#11553 Jason Dillaman)
* librbd: fix image format detection (Zhiqiang Wang)
* librbd: fix lock ordering issue (#11577 Jason Dillaman)
* librbd: fix reads larger than the cache size (Lu Shi)
* librbd: fix snapshot creation when other snap is active (#11475 Jason Dillaman)
* librbd: flatten/copyup fixes (Jason Dillaman)
* librbd: handle NOCACHE fadvise flag (Jinapeng Ma)
* librbd: lockdep, helgrind validation (Jason Dillaman, Josh Durgin)
* librbd: metadata filter fixes (Haomai Wang)
* librbd: misc aio fixes (#5488 Jason Dillaman)
* librbd: misc rbd fixes (#11478 #11113 #11342 #11380 Jason Dillaman, Zhiqiang Wang)
* librbd: new diff_iterate2 API (Jason Dillaman)
* librbd: object map rebuild support (Jason Dillaman)
* librbd: only update image flags while hold exclusive lock (#11791 Jason Dillaman)
* librbd: optionally disable allocation hint (Haomai Wang)
* librbd: prevent race between resize requests (#12664 Jason Dillaman)
* librbd: readahead fixes (Zhiqiang Wang)
* librbd: return result code from close (#12069 Jason Dillaman)
* librbd: store metadata, including config options, in image (Haomai Wang)
* librbd: tolerate old osds when getting image metadata (#11549 Jason Dillaman)
* librbd: use write_full when possible (Zhiqiang Wang)
* log: fix data corruption race resulting from log rotation (#12465 Samuel Just)
* logrotate.d: prefer service over invoke-rc.d (#11330 Win Hierman, Sage Weil)
* mds: add 'damaged' state to MDSMap (John Spray)
* mds: add nicknames for perfcounters (John Spray)
* mds: avoid emitting cap warnigns before evicting session (John Spray)
* mds: avoid getting stuck in XLOCKDONE (#11254 Yan, Zheng)
* mds: disable problematic rstat propagation into snap parents (Yan, Zheng)
* mds: do not add snapped items to bloom filter (Yan, Zheng)
* mds: expose frags via asok (John Spray)
* mds: fix expected holes in journal objects (#13167 Yan, Zheng)
* mds: fix handling for missing mydir dirfrag (#11641 John Spray)
* mds: fix integer truncateion on large client ids (Henry Chang)
* mds: fix mydir replica issue with shutdown (#10743 John Spray)
* mds: fix out-of-order messages (#11258 Yan, Zheng)
* mds: fix rejoin (Yan, Zheng)
* mds: fix setting entire file layout in one setxattr (John Spray)
* mds: fix shutdown (John Spray)
* mds: fix shutdown with strays (#10744 John Spray)
* mds: fix SnapServer crash on deleted pool (John Spray)
* mds: fix snapshot bugs (Yan, Zheng)
* mds: fix stray reintegration (Yan, Zheng)
* mds: fix stray handling (John Spray)
* mds: fix suicide beacon (John Spray)
* mds: flush immediately in do_open_truncate (#11011 John Spray)
* mds: handle misc corruption issues (John Spray)
* mds: improve dump methods (John Spray)
* mds: many fixes (Yan, Zheng, John Spray, Greg Farnum)
* mds: many snapshot and stray fixes (Yan, Zheng)
* mds: misc fixes (Jianpeng Ma, Dan van der Ster, Zhang Zhi)
* mds: misc journal cleanups and fixes (#10368 John Spray)
* mds: misc repair improvements (John Spray)
* mds: misc snap fixes (Zheng Yan)
* mds: misc snapshot fixes (Yan, Zheng)
* mds: new SessionMap storage using omap (#10649 John Spray)
* mds: persist completed_requests reliably (#11048 John Spray)
* mds: reduce memory consumption (Yan, Zheng)
* mds: respawn instead of suicide on blacklist (John Spray)
* mds: separate safe_pos in Journaler (#10368 John Spray)
* mds: snapshot rename support (#3645 Yan, Zheng)
* mds: store layout on header object (#4161 John Spray)
* mds: throttle purge stray operations (#10390 John Spray)
* mds: tolerate clock jumping backwards (#11053 Yan, Zheng)
* mds: warn when clients fail to advance oldest_client_tid (#10657 Yan, Zheng)
* misc cleanups and fixes (Danny Al-Gaaf)
* misc coverity fixes (Danny Al-Gaaf)
* misc performance and cleanup (Nathan Cutler, Xinxin Shu)
* mon: add cache over MonitorDBStore (Kefu Chai)
* mon: add 'mon_metadata <id>' command (Kefu Chai)
* mon: add 'node ls ...' command (Kefu Chai)
* mon: add NOFORWARD, OBSOLETE, DEPRECATE flags for mon commands (Joao Eduardo Luis)
* mon: add PG count to 'ceph osd df' output (Michal Jarzabek)
* mon: 'ceph osd metadata' can dump all osds (Haomai Wang)
* mon: clean up, reorg some mon commands (Joao Eduardo Luis)
* monclient: flush_log (John Spray)
* mon: detect kv backend failures (Sage Weil)
* mon: disallow >2 tiers (#11840 Kefu Chai)
* mon: disallow ec pools as tiers (#11650 Samuel Just)
* mon: do not deactivate last mds (#10862 John Spray)
* mon: fix average utilization calc for 'osd df' (Mykola Golub)
* mon: fix CRUSH map test for new pools (Sage Weil)
* mon: fix log dump crash when debugging (Mykola Golub)
* mon: fix mds beacon replies (#11590 Kefu Chai)
* mon: fix metadata update race (Mykola Golub)
* mon: fix min_last_epoch_clean tracking (Kefu Chai)
* mon: fix 'pg ls' sort order, state names (#11569 Kefu Chai)
* mon: fix refresh (#11470 Joao Eduardo Luis)
* mon: fix variance calc in 'osd df' (Sage Weil)
* mon: improve callout to crushtool (Mykola Golub)
* mon: make blocked op messages more readable (Jianpeng Ma)
* mon: make osd get pool 'all' only return applicable fields (#10891 Michal Jarzabek)
* mon: misc scaling fixes (Sage Weil)
* mon: normalize erasure-code profile for storage and comparison (Loic Dachary)
* mon: only send mon metadata to supporting peers (Sage Weil)
* mon: optionally specify osd id on 'osd create' (Mykola Golub)
* mon: 'osd tree' fixes (Kefu Chai)
* mon: periodic background scrub (Joao Eduardo Luis)
* mon: prevent bucket deletion when referenced by a crush rule (#11602 Sage Weil)
* mon: prevent pgp_num > pg_num (#12025 Xinxin Shu)
* mon: prevent pool with snapshot state from being used as a tier (#11493 Sage Weil)
* mon: prime pg_temp when CRUSH map changes (Sage Weil)
* mon: refine check_remove_tier checks (#11504 John Spray)
* mon: reject large max_mds values (#12222 John Spray)
* mon: remove spurious who arg from 'mds rm ...' (John Spray)
* mon: streamline session handling, fix memory leaks (Sage Weil)
* mon: upgrades must pass through hammer (Sage Weil)
* mon: warn on bogus cache tier config (Jianpeng Ma)
* msgr: add ceph_perf_msgr tool (Hoamai Wang)
* msgr: async: fix seq handling (Haomai Wang)
* msgr: async: many many fixes (Haomai Wang)
* msgr: simple: fix clear_pipe (#11381 Haomai Wang)
* msgr: simple: fix connect_seq assert (Haomai Wang)
* msgr: xio: fastpath improvements (Raju Kurunkad)
* msgr: xio: fix ip and nonce (Raju Kurunkad)
* msgr: xio: improve lane assignment (Vu Pham)
* msgr: xio: sync with accellio v1.4 (Vu Pham)
* msgr: xio: misc fixes (#10735 Matt Benjamin, Kefu Chai, Danny Al-Gaaf, Raju Kurunkad, Vu Pham, Casey Bodley)
* msg: unit tests (Haomai Wang)
* objectcacher: misc bug fixes (Jianpeng Ma)
* osd: add latency perf counters for tier operations (Xinze Chi)
* osd: add misc perfcounters (Xinze Chi)
* osd: add simple sleep injection in recovery (Sage Weil)
* osd: allow SEEK_HOLE/SEEK_DATA for sparse read (Zhiqiang Wang)
* osd: avoid dup omap sets for in pg metadata (Sage Weil)
* osd: avoid multiple hit set insertions (Zhiqiang Wang)
* osd: avoid transaction append in some cases (Sage Weil)
* osd: break PG removal into multiple iterations (#10198 Guang Yang)
* osd: cache proxy-write support (Zhiqiang Wang, Samuel Just)
* osd: check scrub state when handling map (Jianpeng Ma)
* osd: clean up some constness, privateness (Kefu Chai)
* osd: clean up temp object if promotion fails (Jianpeng Ma)
* osd: configure promotion based on write recency (Zhiqiang Wang)
* osd: constrain collections to meta and PGs (normal and temp) (Sage Weil)
* osd: don't send dup MMonGetOSDMap requests (Sage Weil, Kefu Chai)
* osd: EIO injection (David Zhang)
* osd: elminiate txn apend, ECSubWrite copy (Samuel Just)
* osd: erasure-code: drop entries according to LRU (Andreas-Joachim Peters)
* osd: erasure-code: fix SHEC floating point bug (#12936 Loic Dachary)
* osd: erasure-code: update to ISA-L 2.14 (Yuan Zhou)
* osd: filejournal: cleanup (David Zafman)
* osd: filestore: clone using splice (Jianpeng Ma)
* osd: filestore: fix recursive lock (Xinxin Shu)
* osd: fix check_for_full (Henry Chang)
* osd: fix dirty accounting in make_writeable (Zhiqiang Wang)
* osd: fix dup promotion lost op bug (Zhiqiang Wang)
* osd: fix endless repair when object is unrecoverable (Jianpeng Ma, Kefu Chai)
* osd: fix hitset object naming to use GMT (Kefu Chai)
* osd: fix misc memory leaks (Sage Weil)
* osd: fix negative degraded stats during backfill (Guang Yang)
* osd: fix osdmap dump of blacklist items (John Spray)
* osd: fix peek_queue locking in FileStore (Xinze Chi)
* osd: fix pg resurrection (#11429 Samuel Just)
* osd: fix promotion vs full cache tier (Samuel Just)
* osd: fix replay requeue when pg is still activating (#13116 Samuel Just)
* osd: fix scrub stat bugs (Sage Weil, Samuel Just)
* osd: fix snap flushing from cache tier (again) (#11787 Samuel Just)
* osd: fix snap handling on promotion (#11296 Sam Just)
* osd: fix temp-clearing (David Zafman)
* osd: force promotion for ops EC can't handle (Zhiqiang Wang)
* osd: handle log split with overlapping entries (#11358 Samuel Just)
* osd: ignore non-existent osds in unfound calc (#10976 Mykola Golub)
* osd: improve behavior on machines with large memory pages (Steve Capper)
* osd: include a temp namespace within each collection/pgid (Sage Weil)
* osd: increase default max open files (Owen Synge)
* osd: keyvaluestore: misc fixes (Varada Kari)
* osd: low and high speed flush modes (Mingxin Liu)
* osd: make suicide timeouts individually configurable (Samuel Just)
* osd: merge multiple setattr calls into a setattrs call (Xinxin Shu)
* osd: misc fixes (Ning Yao, Kefu Chai, Xinze Chi, Zhiqiang Wang, Jianpeng Ma)
* osd: move scrub in OpWQ (Samuel Just)
* osd: newstore prototype (Sage Weil)
* osd: ObjectStore internal API refactor (Sage Weil)
* osd: peer_features includes self (David Zafman)
* osd: pool size change triggers new interval (#11771 Samuel Just)
* osd: prepopulate needs_recovery_map when only one peer has missing (#9558 Guang Yang)
* osd: randomize scrub times (#10973 Kefu Chai)
* osd: recovery, peering fixes (#11687 Samuel Just)
* osd: refactor scrub and digest recording (Sage Weil)
* osd: refuse first write to EC object at non-zero offset (Jianpeng Ma)
* osd: relax reply order on proxy read (#11211 Zhiqiang Wang)
* osd: require firefly features (David Zafman)
* osd: set initial crush weight with more precision (Sage Weil)
* osd: SHEC no longer experimental
* osd: skip promotion for flush/evict op (Zhiqiang Wang)
* osd: stripe over small xattrs to fit in XFS's 255 byte inline limit (Sage Weil, Ning Yao)
* osd: sync object_map on syncfs (Samuel Just)
* osd: take excl lock of op is rw (Samuel Just)
* osd: throttle evict ops (Yunchuan Wen)
* osd: upgrades must pass through hammer (Sage Weil)
* osd: use a temp object for recovery (Sage Weil)
* osd: use blkid to collection partition information (Joseph Handzik)
* osd: use SEEK_HOLE / SEEK_DATA for sparse copy (Xinxin Shu)
* osd: WBThrottle cleanups (Jianpeng Ma)
* osd: write journal header on clean shutdown (Xinze Chi)
* osdc/Objecter: allow per-pool calls to op_cancel_writes (John Spray)
* os/filestore: enlarge getxattr buffer size (Jianpeng Ma)
* pybind: pep8 cleanups (Danny Al-Gaaf)
* pycephfs: many fixes for bindings (Haomai Wang)
* qa: fix filelock_interrupt.py test (Yan, Zheng)
* qa: improve ceph-disk tests (Loic Dachary)
* qa: improve docker build layers (Loic Dachary)
* qa: run-make-check.sh script (Loic Dachary)
* rados: add --striper option to use libradosstriper (#10759 Sebastien Ponce)
* rados: bench: add --no-verify option to improve performance (Piotr Dalek)
* rados bench: misc fixes (Dmitry Yatsushkevich)
* rados: fix error message on failed pool removal (Wido den Hollander)
* radosgw-admin: add 'bucket check' function to repair bucket index (Yehuda Sadeh)
* radosgw-admin: fix subuser modify output (#12286 Guce)
* rados: handle --snapid arg properly (Abhishek Lekshmanan)
* rados: improve bench buffer handling, performance (Piotr Dalek)
* rados: misc bench fixes (Dmitry Yatsushkevich)
* rados: new pool import implementation (John Spray)
* rados: translate errno to string in CLI (#10877 Kefu Chai)
* rbd: accept map options config option (Ilya Dryomov)
* rbd: add disk usage tool (#7746 Jason Dillaman)
* rbd: allow unmapping by spec (Ilya Dryomov)
* rbd: cli: fix arg parsing with --io-pattern (Dmitry Yatsushkevich)
* rbd: deprecate --new-format option (Jason Dillman)
* rbd: fix error messages (#2862 Rajesh Nambiar)
* rbd: fix link issues (Jason Dillaman)
* rbd: improve CLI arg parsing, usage (Ilya Dryomov)
* rbd: rbd-replay-prep and rbd-replay improvements (Jason Dillaman)
* rbd: recognize queue_depth kernel option (Ilya Dryomov)
* rbd: support G and T units for CLI (Abhishek Lekshmanan)
* rbd: update rbd man page (Ilya Dryomov)
* rbd: update xfstests tests (Douglas Fuller)
* rbd: use image-spec and snap-spec in help (Vikhyat Umrao, Ilya Dryomov)
* rest-bench: misc fixes (Shawn Chen)
* rest-bench: support https (#3968 Yuan Zhou)
* rgw: add max multipart upload parts (#12146 Abshishek Dixit)
* rgw: add missing headers to Swift container details (#10666 Ahmad Faheem, Dmytro Iurchenko)
* rgw: add stats to headers for account GET (#10684 Yuan Zhou)
* rgw: add Trasnaction-Id to response (Abhishek Dixit)
* rgw: add X-Timestamp for Swift containers (#10938 Radoslaw Zarzynski)
* rgw: always check if token is expired (#11367 Anton Aksola, Riku Lehto)
* rgw: conversion tool to repair broken multipart objects (#12079 Yehuda Sadeh)
* rgw: document layout of pools and objects (Pete Zaitcev)
* rgw: do not enclose bucket header in quotes (#11860 Wido den Hollander)
* rgw: do not prefetch data for HEAD requests (Guang Yang)
* rgw: do not preserve ACLs when copying object (#12370 Yehuda Sadeh)
* rgw: do not set content-type if length is 0 (#11091 Orit Wasserman)
* rgw: don't clobber bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: don't use end_marker for namespaced object listing (#11437 Yehuda Sadeh)
* rgw: don't use rgw_socket_path if frontend is configured (#11160 Yehuda Sadeh)
* rgw: enforce Content-Length for POST on Swift cont/obj (#10661 Radoslaw Zarzynski)
* rgw: error out if frontend did not send all data (#11851 Yehuda Sadeh)
* rgw: expose the number of unhealthy workers through admin socket (Guang Yang)
* rgw: fail if parts not specified on multipart upload (#11435 Yehuda Sadeh)
* rgw: fix assignment of copy obj attributes (#11563 Yehuda Sadeh)
* rgw: fix broken stats in container listing (#11285 Radoslaw Zarzynski)
* rgw: fix bug in domain/subdomain splitting (Robin H. Johnson)
* rgw: fix casing of Content-Type header (Robin H. Johnson)
* rgw: fix civetweb max threads (#10243 Yehuda Sadeh)
* rgw: fix Connection: header handling (#12298 Wido den Hollander)
* rgw: fix copy metadata, support X-Copied-From for swift (#10663 Radoslaw Zarzynski)
* rgw: fix data corruptions race condition (#11749 Wuxingyi)
* rgw: fix decoding of X-Object-Manifest from GET on Swift DLO (Radslow Rzarzynski)
* rgw: fix GET on swift account when limit == 0 (#10683 Radoslaw Zarzynski)
* rgw: fix handling empty metadata items on Swift container (#11088 Radoslaw Zarzynski)
* rgw: fix JSON response when getting user quota (#12117 Wuxingyi)
* rgw: fix locator for objects starting with _ (#11442 Yehuda Sadeh)
* rgw: fix log rotation (Wuxingyi)
* rgw: fix mulitipart upload in retry path (#11604 Yehuda Sadeh)
* rgw: fix quota enforcement on POST (#11323 Sergey Arkhipov)
* rgw: fix reset_loc (#11974 Yehuda Sadeh)
* rgw: fix return code on missing upload (#11436 Yehuda Sadeh)
* rgw: fix sysvinit script
* rgw: fix sysvinit script w/ multiple instances (Sage Weil, Pavan Rallabhandi)
* rgw: force content_type for swift bucket stats requests (#12095 Orit Wasserman)
* rgw: force content type header on responses with no body (#11438 Orit Wasserman)
* rgw: generate Date header for civetweb (#10873 Radoslaw Zarzynski)
* rgw: generate new object tag when setting attrs (#11256 Yehuda Sadeh)
* rgw: improve content-length env var handling (#11419 Robin H. Johnson)
* rgw: improved support for swift account metadata (Radoslaw Zarzynski)
* rgw: improve handling of already removed buckets in expirer (Radoslaw Rzarzynski)
* rgw: issue aio for first chunk before flush cached data (#11322 Guang Yang)
* rgw: log to /var/log/ceph instead of /var/log/radosgw
* rgw: make init script wait for radosgw to stop (#11140 Dmitry Yatsushkevich)
* rgw: make max put size configurable (#6999 Yuan Zhou)
* rgw: make quota/gc threads configurable (#11047 Guang Yang)
* rgw: make read user buckets backward compat (#10683 Radoslaw Zarzynski)
* rgw: merge manifests properly with prefix override (#11622 Yehuda Sadeh)
* rgw: only scan for objects not in a namespace (#11984 Yehuda Sadeh)
* rgw: orphan detection tool (Yehuda Sadeh)
* rgw: pass in civetweb configurables (#10907 Yehuda Sadeh)
* rgw: rectify 202 Accepted in PUT response (#11148 Radoslaw Zarzynski)
* rgw: remove meta file after deleting bucket (#11149 Orit Wasserman)
* rgw: remove trailing :port from HTTP_HOST header (Sage Weil)
* rgw: return 412 on bad limit when listing buckets (#11613 Yehuda Sadeh)
* rgw: rework X-Trans-Id header to conform with Swift API (Radoslaw Rzarzynski)
* rgw: s3 encoding-type for get bucket (Jeff Weber)
* rgw: send ETag, Last-Modified for swift (#11087 Radoslaw Zarzynski)
* rgw: set content length on container GET, PUT, DELETE, HEAD (#10971, #11036 Radoslaw Zarzynski)
* rgw: set max buckets per user in ceph.conf (Vikhyat Umrao)
* rgw: shard work over multiple librados instances (Pavan Rallabhandi)
* rgw: support end marker on swift container GET (#10682 Radoslaw Zarzynski)
* rgw: support for Swift expiration API (Radoslaw Rzarzynski, Yehuda Sadeh)
* rgw: swift: allow setting attributes with COPY (#10662 Ahmad Faheem, Dmytro Iurchenko)
* rgw: swift: do not override sent content type (#12363 Orit Wasserman)
* rgw: swift: enforce Content-Type in response (#12157 Radoslaw Zarzynski)
* rgw: swift: fix account listing (#11501 Radoslaw Zarzynski)
* rgw: swift: fix metadata handling on copy (#10645 Radoslaw Zarzynski)
* rgw: swift: send Last-Modified header (#10650 Radoslaw Zarzynski)
* rgw: swift: set Content-Length for account GET (#12158 Radoslav Zarzynski)
* rgw: swift: set content-length on keystone tokens (#11473 Herv Rousseau)
* rgw: update keystone cache with token info (#11125 Yehuda Sadeh)
* rgw: update to latest civetweb, enable config for IPv6 (#10965 Yehuda Sadeh)
* rgw: use attrs from source bucket on copy (#11639 Javier M. Mellid)
* rgw: use correct oid for gc chains (#11447 Yehuda Sadeh)
* rgw: user rm is idempotent (Orit Wasserman)
* rgw: use unique request id for civetweb (#10295 Orit Wasserman)
* rocksdb: add perf counters for get/put latency (Xinxin Shu)
* rocksdb, leveldb: fix compact_on_mount (Xiaoxi Chen)
* rocksdb: pass options as single string (Xiaoxi Chen)
* rocksdb: update to latest (Xiaoxi Chen)
* rpm: add suse firewall files (Tim Serong)
* rpm: always rebuild and install man pages for rpm (Owen Synge)
* rpm: loosen ceph-test dependencies (Ken Dreyer)
* rpm: many spec file fixes (Owen Synge, Ken Dreyer)
* rpm: misc fixes (Boris Ranto, Owen Synge, Ken Dreyer, Ira Cooper)
* rpm: misc systemd and SUSE fixes (Owen Synge, Nathan Cutler)
* selinux policy (Boris Ranto, Milan Broz)
* systemd: logrotate fixes (Tim Serong, Lars Marowsky-Bree, Nathan Cutler)
* systemd: many fixes (Sage Weil, Owen Synge, Boris Ranto, Dan van der Ster)
* systemd: run daemons as user ceph
* sysvinit compat: misc fixes (Owen Synge)
* test: misc fs test improvements (John Spray, Loic Dachary)
* test: python tests, linter cleanup (Alfredo Deza)
* tests: fixes for rbd xstests (Douglas Fuller)
* tests: fix tiering health checks (Loic Dachary)
* tests for low-level performance (Haomai Wang)
* tests: many ec non-regression improvements (Loic Dachary)
* tests: many many ec test improvements (Loic Dachary)
* upstart: throttle restarts (#11798 Sage Weil, Greg Farnum)


v9.1.0 Infernalis release candidate
===================================

This is the first Infernalis release candidate.  There have been some
major changes since Hammer, and the upgrade process is non-trivial.
Please read carefully.

Getting the release candidate
-----------------------------

The v9.1.0 packages are pushed to the development release repositories::

  http://download.ceph.com/rpm-testing
  http://download.ceph.com/debian-testing

For for info, see::

  http://docs.ceph.com/docs/master/install/get-packages/

Or install with ceph-deploy via::

  ceph-deploy install --testing HOST


Known issues
------------

* librbd and librados ABI compatibility is broken.  Be careful
  installing this RC on client machines (e.g., those running qemu).
  It will be fixed in the final v9.2.0 release.


Major Changes from Hammer
-------------------------

- *General*:

  * Ceph daemons are now managed via systemd (with the exception of
    Ubuntu Trusty, which still uses upstart).
  * Ceph daemons run as 'ceph' user instead of root.
  * On Red Hat distros, there is also an SELinux policy.

- *RADOS*:

  * The RADOS cache tier can now proxy write operations to the base
    tier, allowing writes to be handled without forcing migration of
    an object into the cache.
  * The SHEC erasure coding support is no longer flagged as
    experimental. SHEC trades some additional storage space for faster
    repair.
  * There is now a unified queue (and thus prioritization) of client
    IO, scrubbing, and snapshot trimming.
  * There have been many improvements to low-level repair tooling
    (ceph-objectstore-tool).
  * The internal ObjectStore API has been significantly cleaned up in order
    to faciliate new storage backends like NewStore.

- *RGW*:

  * The Swift API now supports object expiration.
  * There are many Swift API compatibility improvements.

- *RBD*:

  * The ``rbd du`` command shows actual usage (quickly, when
    object-map is enabled).
  * The object-map feature has seen many stability improvements.
  * Object-map and exclusive-lock features can be enabled or disabled
    dynamically.
  * You can now store user metadata and set persistent librbd options
    associated with individual images.
  * The new deep-flatten features allows flattening of a clone and all
    of its snapshots.  (Previously snapshots could not be flattened.)
  * The export-diff command command is now faster (it uses aio).  There is also
    a new fast-diff feature.
  * The --size argument can be specified with a suffix for units
    (e.g., ``--size 64G``).
  * There is a new ``rbd status`` command that, for now, shows who has
    the image open/mapped.

- *CephFS*:

  * You can now rename snapshots.
  * There have been ongoing improvements around administration, diagnostics,
    and the check and repair tools.
  * The caching and revocation of client cache state due to unused
    inodes has been dramatically improved.
  * The ceph-fuse client behaves better on 32-bit hosts.

Distro compatibility
--------------------

We have decided to drop support for many older distributions so that we can
move to a newer compiler toolchain (e.g., C++11).  Although it is still possible
to build Ceph on older distributions by installing backported development tools,
we are not building and publishing release packages for them on ceph.com.

In particular,

* CentOS 7 or later; we have dropped support for CentOS 6 (and other
  RHEL 6 derivatives, like Scientific Linux 6).
* Debian Jessie 8.x or later; Debian Wheezy 7.x's g++ has incomplete
  support for C++11 (and no systemd).
* Ubuntu Trusty 14.04 or later; Ubuntu Precise 12.04 is no longer
  supported.
* Fedora 22 or later.

Upgrading from Firefly
----------------------

Upgrading directly from Firefly v0.80.z is not possible.  All clusters
must first upgrade to Hammer v0.94.4 or a later v0.94.z release; only
then is it possible to do online upgrade to Infernalis 9.2.z.

User can upgrade to latest hammer v0.94.z
from gitbuilder with(also refer the hammer release notes for more details)::

  ceph-deploy install --release hammer HOST


Upgrading from Hammer
---------------------

* All cluster nodes must first upgrade to Hammer v0.94.4 or a later v0.94.z release; only
  then is it possible to do online upgrade to Infernalis 9.2.z.

* For all distributions that support systemd (CentOS 7, Fedora, Debian
  Jessie 8.x, OpenSUSE), ceph daemons are now managed using native systemd
  files instead of the legacy sysvinit scripts.  For example::

    systemctl start ceph.target       # start all daemons
    systemctl status ceph-osd@12      # check status of osd.12

  The main notable distro that is *not* yet using systemd is Ubuntu trusty
  14.04.  (The next Ubuntu LTS, 16.04, will use systemd instead of upstart.)

* Ceph daemons now run as user and group ``ceph`` by default.  The
  ceph user has a static UID assigned by Fedora and Debian (also used
  by derivative distributions like RHEL/CentOS and Ubuntu).  On SUSE
  the ceph user will currently get a dynamically assigned UID when the
  user is created.

  If your systems already have a ceph user, the package upgrade
  process will usually fail with an error.  We suggest you first
  remove or rename the existing 'ceph' user and then upgrade.

  When upgrading, administrators have two options:

   #. Add the following line to ``ceph.conf`` on all hosts::

        setuser match path = /var/lib/ceph/$type/$cluster-$id

      This will make the Ceph daemons run as root (i.e., not drop
      privileges and switch to user ceph) if the daemon's data
      directory is still owned by root.  Newly deployed daemons will
      be created with data owned by user ceph and will run with
      reduced privileges, but upgraded daemons will continue to run as
      root.

   #. Fix the data ownership during the upgrade.  This is the preferred option,
      but is more work.  The process for each host would be to:

      #. Upgrade the ceph package.  This creates the ceph user and group.  For
         example::

           ceph-deploy install --stable infernalis HOST

      #. Stop the daemon(s)::

           service ceph stop           # fedora, centos, rhel, debian
           stop ceph-all               # ubuntu

      #. Fix the ownership::

           chown -R ceph:ceph /var/lib/ceph
           chown -R ceph:ceph /var/log/ceph

      #. Restart the daemon(s)::

           start ceph-all                # ubuntu
           systemctl start ceph.target   # debian, centos, fedora, rhel

* The on-disk format for the experimental KeyValueStore OSD backend has
  changed.  You will need to remove any OSDs using that backend before you
  upgrade any test clusters that use it.

Upgrade notes
-------------

* When a pool quota is reached, librados operations now block indefinitely,
  the same way they do when the cluster fills up.  (Previously they would return
  -ENOSPC).  By default, a full cluster or pool will now block.  If your
  librados application can handle ENOSPC or EDQUOT errors gracefully, you can
  get error returns instead by using the new librados OPERATION_FULL_TRY flag.

Notable changes
---------------

NOTE: These notes are somewhat abbreviated while we find a less
time-consuming process for generating them.

* build: C++11 now supported
* build: many cmake improvements
* build: OSX build fixes (Yan, Zheng)
* build: remove rest-bench
* ceph-disk: many fixes (Loic Dachary)
* ceph-disk: support for multipath devices (Loic Dachary)
* ceph-fuse: mostly behave on 32-bit hosts (Yan, Zheng)
* ceph-objectstore-tool: many improvements (David Zafman)
* common: bufferlist performance tuning (Piotr Dalek, Sage Weil)
* common: make mutex more efficient
* common: some async compression infrastructure (Haomai Wang)
* librados: add FULL_TRY and FULL_FORCE flags for dealing with full clusters or pools (Sage Weil)
* librados: fix notify completion race (#13114 Sage Weil)
* librados, libcephfs: randomize client nonces (Josh Durgin)
* librados: pybind: fix binary omap values (Robin H. Johnson)
* librbd: fix reads larger than the cache size (Lu Shi)
* librbd: metadata filter fixes (Haomai Wang)
* librbd: use write_full when possible (Zhiqiang Wang)
* mds: avoid emitting cap warnigns before evicting session (John Spray)
* mds: fix expected holes in journal objects (#13167 Yan, Zheng)
* mds: fix SnapServer crash on deleted pool (John Spray)
* mds: many fixes (Yan, Zheng, John Spray, Greg Farnum)
* mon: add cache over MonitorDBStore (Kefu Chai)
* mon: 'ceph osd metadata' can dump all osds (Haomai Wang)
* mon: detect kv backend failures (Sage Weil)
* mon: fix CRUSH map test for new pools (Sage Weil)
* mon: fix min_last_epoch_clean tracking (Kefu Chai)
* mon: misc scaling fixes (Sage Weil)
* mon: streamline session handling, fix memory leaks (Sage Weil)
* mon: upgrades must pass through hammer (Sage Weil)
* msg/async: many fixes (Haomai Wang)
* osd: cache proxy-write support (Zhiqiang Wang, Samuel Just)
* osd: configure promotion based on write recency (Zhiqiang Wang)
* osd: don't send dup MMonGetOSDMap requests (Sage Weil, Kefu Chai)
* osd: erasure-code: fix SHEC floating point bug (#12936 Loic Dachary)
* osd: erasure-code: update to ISA-L 2.14 (Yuan Zhou)
* osd: fix hitset object naming to use GMT (Kefu Chai)
* osd: fix misc memory leaks (Sage Weil)
* osd: fix peek_queue locking in FileStore (Xinze Chi)
* osd: fix promotion vs full cache tier (Samuel Just)
* osd: fix replay requeue when pg is still activating (#13116 Samuel Just)
* osd: fix scrub stat bugs (Sage Weil, Samuel Just)
* osd: force promotion for ops EC can't handle (Zhiqiang Wang)
* osd: improve behavior on machines with large memory pages (Steve Capper)
* osd: merge multiple setattr calls into a setattrs call (Xinxin Shu)
* osd: newstore prototype (Sage Weil)
* osd: ObjectStore internal API refactor (Sage Weil)
* osd: SHEC no longer experimental
* osd: throttle evict ops (Yunchuan Wen)
* osd: upgrades must pass through hammer (Sage Weil)
* osd: use SEEK_HOLE / SEEK_DATA for sparse copy (Xinxin Shu)
* rbd: rbd-replay-prep and rbd-replay improvements (Jason Dillaman)
* rgw: expose the number of unhealthy workers through admin socket (Guang Yang)
* rgw: fix casing of Content-Type header (Robin H. Johnson)
* rgw: fix decoding of X-Object-Manifest from GET on Swift DLO (Radslow Rzarzynski)
* rgw: fix sysvinit script
* rgw: fix sysvinit script w/ multiple instances (Sage Weil, Pavan Rallabhandi)
* rgw: improve handling of already removed buckets in expirer (Radoslaw Rzarzynski)
* rgw: log to /var/log/ceph instead of /var/log/radosgw
* rgw: rework X-Trans-Id header to be conform with Swift API (Radoslaw Rzarzynski)
* rgw: s3 encoding-type for get bucket (Jeff Weber)
* rgw: set max buckets per user in ceph.conf (Vikhyat Umrao)
* rgw: support for Swift expiration API (Radoslaw Rzarzynski, Yehuda Sadeh)
* rgw: user rm is idempotent (Orit Wasserman)
* selinux policy (Boris Ranto, Milan Broz)
* systemd: many fixes (Sage Weil, Owen Synge, Boris Ranto, Dan van der Ster)
* systemd: run daemons as user ceph


v9.0.3
======

This is the second to last batch of development work for the
Infernalis cycle.  The most intrusive change is an internal (non
user-visible) change to the OSD's ObjectStore interface.  Many fixes and
improvements elsewhere across RGW, RBD, and another big pile of CephFS
scrub/repair improvements.

Upgrading
---------

* The return code for librbd's rbd_aio_read and Image::aio_read API methods no
  longer returns the number of bytes read upon success.  Instead, it returns 0
  upon success and a negative value upon failure.

* 'ceph scrub', 'ceph compact' and 'ceph sync force' are now deprecated.  Users
  should instead use 'ceph mon scrub', 'ceph mon compact' and
  'ceph mon sync force'.

* 'ceph mon_metadata' should now be used as 'ceph mon metadata'.

* The `--dump-json` option of "osdmaptool" is replaced by `--dump json`.

* The commands of 'pg ls-by-{pool,primary,osd}' and 'pg ls' now take 'recovering'
  instead of 'recovery' to include the recovering pgs in the listed pgs.


Notable Changes
---------------

  * autotools: fix out of tree build (Krxysztof Kosinski)
  * autotools: improve make check output (Loic Dachary)
  * buffer: add invalidate_crc() (Piotr Dalek)
  * buffer: fix zero bug (#12252 Haomai Wang)
  * build: fix junit detection on Fedora 22 (Ira Cooper)
  * ceph-disk: install pip > 6.1 (#11952 Loic Dachary)
  * cephfs-data-scan: many additions, improvements (John Spray)
  * ceph: improve error output for 'tell' (#11101 Kefu Chai)
  * ceph-objectstore-tool: misc improvements (David Zafman)
  * ceph-objectstore-tool: refactoring and cleanup (John Spray)
  * ceph_test_rados: test pipelined reads (Zhiqiang Wang)
  * common: fix bit_vector extent calc (#12611 Jason Dillaman)
  * common: make work queue addition/removal thread safe (#12662 Jason Dillaman)
  * common: optracker improvements (Zhiqiang Wang, Jianpeng Ma)
  * crush: add --check to validate dangling names, max osd id (Kefu Chai)
  * crush: cleanup, sync with kernel (Ilya Dryomov)
  * crush: fix subtree base weight on adjust_subtree_weight (#11855 Sage Weil)
  * crypo: fix NSS leak (Jason Dillaman)
  * crypto: fix unbalanced init/shutdown (#12598 Zheng Yan)
  * doc: misc updates (Kefu Chai, Owen Synge, Gael Fenet-Garde, Loic Dachary, Yannick Atchy-Dalama, Jiaying Ren, Kevin Caradant, Robert Maxime, Nicolas Yong, Germain Chipaux, Arthur Gorjux, Gabriel Sentucq, Clement Lebrun, Jean-Remi Deveaux, Clair Massot, Robin Tang, Thomas Laumondais, Jordan Dorne, Yuan Zhou, Valentin Thomas, Pierre Chaumont, Benjamin Troquereau, Benjamin Sesia, Vikhyat Umrao)
  * erasure-code: cleanup (Kefu Chai)
  * erasure-code: improve tests (Loic Dachary)
  * erasure-code: shec: fix recovery bugs (Takanori Nakao, Shotaro Kawaguchi)
  * libcephfs: add pread, pwrite (Jevon Qiao)
  * libcephfs,ceph-fuse: cache cleanup (Zheng Yan)
  * librados: add src_fadvise_flags for copy-from (Jianpeng Ma)
  * librados: respect default_crush_ruleset on pool_create (#11640 Yuan Zhou)
  * librbd: fadvise for copy, export, import (Jianpeng Ma)
  * librbd: handle NOCACHE fadvise flag (Jinapeng Ma)
  * librbd: optionally disable allocation hint (Haomai Wang)
  * librbd: prevent race between resize requests (#12664 Jason Dillaman)
  * log: fix data corruption race resulting from log rotation (#12465 Samuel Just)
  * mds: expose frags via asok (John Spray)
  * mds: fix setting entire file layout in one setxattr (John Spray)
  * mds: fix shutdown (John Spray)
  * mds: handle misc corruption issues (John Spray)
  * mds: misc fixes (Jianpeng Ma, Dan van der Ster, Zhang Zhi)
  * mds: misc snap fixes (Zheng Yan)
  * mds: store layout on header object (#4161 John Spray)
  * misc performance and cleanup (Nathan Cutler, Xinxin Shu)
  * mon: add NOFORWARD, OBSOLETE, DEPRECATE flags for mon commands (Joao Eduardo Luis)
  * mon: add PG count to 'ceph osd df' output (Michal Jarzabek)
  * mon: clean up, reorg some mon commands (Joao Eduardo Luis)
  * mon: disallow >2 tiers (#11840 Kefu Chai)
  * mon: fix log dump crash when debugging (Mykola Golub)
  * mon: fix metadata update race (Mykola Golub)
  * mon: fix refresh (#11470 Joao Eduardo Luis)
  * mon: make blocked op messages more readable (Jianpeng Ma)
  * mon: only send mon metadata to supporting peers (Sage Weil)
  * mon: periodic background scrub (Joao Eduardo Luis)
  * mon: prevent pgp_num > pg_num (#12025 Xinxin Shu)
  * mon: reject large max_mds values (#12222 John Spray)
  * msgr: add ceph_perf_msgr tool (Hoamai Wang)
  * msgr: async: fix seq handling (Haomai Wang)
  * msgr: xio: fastpath improvements (Raju Kurunkad)
  * msgr: xio: sync with accellio v1.4 (Vu Pham)
  * osd: clean up temp object if promotion fails (Jianpeng Ma)
  * osd: constrain collections to meta and PGs (normal and temp) (Sage Weil)
  * osd: filestore: clone using splice (Jianpeng Ma)
  * osd: filestore: fix recursive lock (Xinxin Shu)
  * osd: fix dup promotion lost op bug (Zhiqiang Wang)
  * osd: fix temp-clearing (David Zafman)
  * osd: include a temp namespace within each collection/pgid (Sage Weil)
  * osd: low and high speed flush modes (Mingxin Liu)
  * osd: peer_features includes self (David Zafman)
  * osd: recovery, peering fixes (#11687 Samuel Just)
  * osd: require firefly features (David Zafman)
  * osd: set initial crush weight with more precision (Sage Weil)
  * osd: use a temp object for recovery (Sage Weil)
  * osd: use blkid to collection partition information (Joseph Handzik)
  * rados: add --striper option to use libradosstriper (#10759 Sebastien Ponce)
  * radosgw-admin: fix subuser modify output (#12286 Guce)
  * rados: handle --snapid arg properly (Abhishek Lekshmanan)
  * rados: improve bench buffer handling, performance (Piotr Dalek)
  * rados: new pool import implementation (John Spray)
  * rbd: fix link issues (Jason Dillaman)
  * rbd: improve CLI arg parsing, usage (Ilya Dryomov)
  * rbd: recognize queue_depth kernel option (Ilya Dryomov)
  * rbd: support G and T units for CLI (Abhishek Lekshmanan)
  * rbd: use image-spec and snap-spec in help (Vikhyat Umrao, Ilya Dryomov)
  * rest-bench: misc fixes (Shawn Chen)
  * rest-bench: support https (#3968 Yuan Zhou)
  * rgw: add max multipart upload parts (#12146 Abshishek Dixit)
  * rgw: add Trasnaction-Id to response (Abhishek Dixit)
  * rgw: document layout of pools and objects (Pete Zaitcev)
  * rgw: do not preserve ACLs when copying object (#12370 Yehuda Sadeh)
  * rgw: fix Connection: header handling (#12298 Wido den Hollander)
  * rgw: fix data corruptions race condition (#11749 Wuxingyi)
  * rgw: fix JSON response when getting user quota (#12117 Wuxingyi)
  * rgw: force content_type for swift bucket stats requests (#12095 Orit Wasserman)
  * rgw: improved support for swift account metadata (Radoslaw Zarzynski)
  * rgw: make max put size configurable (#6999 Yuan Zhou)
  * rgw: orphan detection tool (Yehuda Sadeh)
  * rgw: swift: do not override sent content type (#12363 Orit Wasserman)
  * rgw: swift: set Content-Length for account GET (#12158 Radoslav Zarzynski)
  * rpm: always rebuild and install man pages for rpm (Owen Synge)
  * rpm: misc fixes (Boris Ranto, Owen Synge, Ken Dreyer, Ira Cooper)
  * systemd: logrotate fixes (Tim Seron, Lars Marowsky-Bree, Nathan Cutler)
  * sysvinit compat: misc fixes (Owen Synge)
  * test: misc fs test improvements (John Spray, Loic Dachary)
  * test: python tests, linter cleanup (Alfredo Deza)


v9.0.2
======

This development release features more of the OSD work queue
unification, randomized osd scrub times, a huge pile of librbd fixes,
more MDS repair and snapshot fixes, and a significant amount of work
on the tests and build infrastructure.

Notable Changes
---------------

* buffer: some cleanup (Michal Jarzabek)
* build: cmake: fix nss linking (Danny Al-Gaaf)
* build: cmake: misc fixes (Orit Wasserman, Casey Bodley)
* build: install-deps: misc fixes (Loic Dachary)
* build: make_dist_tarball.sh (Sage Weil)
* ceph-detect-init: added Linux Mint (Michal Jarzabek)
* ceph-detect-init: robust init system detection (Owen Synge, Loic Dachary)
* ceph-disk: ensure 'zap' only operates on a full disk (#11272 Loic Dachary)
* ceph-disk: misc fixes to respect init system (Loic Dachary, Owen Synge)
* ceph-disk: support NVMe device partitions (#11612 Ilja Slepnev)
* ceph: fix 'df' units (Zhe Zhang)
* ceph: fix parsing in interactive cli mode (#11279 Kefu Chai)
* ceph-objectstore-tool: many many changes (David Zafman)
* ceph-post-file: misc fixes (Joey McDonald, Sage Weil)
* client: avoid sending unnecessary FLUSHSNAP messages (Yan, Zheng)
* client: exclude setfilelock when calculating oldest tid (Yan, Zheng)
* client: fix error handling in check_pool_perm (John Spray)
* client: fsync waits only for inode's caps to flush (Yan, Zheng)
* client: invalidate kernel dcache when cache size exceeds limits (Yan, Zheng)
* client: make fsync wait for unsafe dir operations (Yan, Zheng)
* client: pin lookup dentry to avoid inode being freed (Yan, Zheng)
* common: detect overflow of int config values (#11484 Kefu Chai)
* common: fix json parsing of utf8 (#7387 Tim Serong)
* common: fix leak of pthread_mutexattr (#11762 Ketor Meng)
* crush: respect default replicated ruleset config on map creation (Ilya Dryomov)
* deb, rpm: move ceph-objectstore-tool to ceph (Ken Dreyer)
* doc: man page updates (Kefu Chai)
* doc: misc updates (#11396 Nilamdyuti, Fracois Lafont, Ken Dreyer, Kefu Chai)
* init-radosgw: merge with sysv version; fix enumeration (Sage Weil)
* librados: add config observer (Alistair Strachan)
* librbd: add const for single-client-only features (Josh Durgin)
* librbd: add deep-flatten operation (Jason Dillaman)
* librbd: avoid blocking aio API methods (#11056 Jason Dillaman)
* librbd: fix fast diff bugs (#11553 Jason Dillaman)
* librbd: fix image format detection (Zhiqiang Wang)
* librbd: fix lock ordering issue (#11577 Jason Dillaman)
* librbd: flatten/copyup fixes (Jason Dillaman)
* librbd: lockdep, helgrind validation (Jason Dillaman, Josh Durgin)
* librbd: only update image flags while hold exclusive lock (#11791 Jason Dillaman)
* librbd: return result code from close (#12069 Jason Dillaman)
* librbd: tolerate old osds when getting image metadata (#11549 Jason Dillaman)
* mds: do not add snapped items to bloom filter (Yan, Zheng)
* mds: fix handling for missing mydir dirfrag (#11641 John Spray)
* mds: fix rejoin (Yan, Zheng)
* mds: fix stra reintegration (Yan, Zheng)
* mds: fix suicide beason (John Spray)
* mds: misc repair improvements (John Spray)
* mds: misc snapshot fixes (Yan, Zheng)
* mds: respawn instead of suicide on blacklist (John Spray)
* misc coverity fixes (Danny Al-Gaaf)
* mon: add 'mon_metadata <id>' command (Kefu Chai)
* mon: add 'node ls ...' command (Kefu Chai)
* mon: disallow ec pools as tiers (#11650 Samuel Just)
* mon: fix mds beacon replies (#11590 Kefu Chai)
* mon: fix 'pg ls' sort order, state names (#11569 Kefu Chai)
* mon: normalize erasure-code profile for storage and comparison (Loic Dachary)
* mon: optionally specify osd id on 'osd create' (Mykola Golub)
* mon: 'osd tree' fixes (Kefu Chai)
* mon: prevent pool with snapshot state from being used as a tier (#11493 Sage Weil)
* mon: refine check_remove_tier checks (#11504 John Spray)
* mon: remove spurious who arg from 'mds rm ...' (John Spray)
* msgr: async: misc fixes (Haomai Wang)
* msgr: xio: fix ip and nonce (Raju Kurunkad)
* msgr: xio: improve lane assignment (Vu Pham)
* msgr: xio: misc fixes (Vu Pham, Cosey Bodley)
* osd: avoid transaction append in some cases (Sage Weil)
* osdc/Objecter: allow per-pool calls to op_cancel_writes (John Spray)
* osd: elminiate txn apend, ECSubWrite copy (Samuel Just)
* osd: filejournal: cleanup (David Zafman)
* osd: fix check_for_full (Henry Chang)
* osd: fix dirty accounting in make_writeable (Zhiqiang Wang)
* osd: fix osdmap dump of blacklist items (John Spray)
* osd: fix snap flushing from cache tier (again) (#11787 Samuel Just)
* osd: fix snap handling on promotion (#11296 Sam Just)
* osd: handle log split with overlapping entries (#11358 Samuel Just)
* osd: keyvaluestore: misc fixes (Varada Kari)
* osd: make suicide timeouts individually configurable (Samuel Just)
* osd: move scrub in OpWQ (Samuel Just)
* osd: pool size change triggers new interval (#11771 Samuel Just)
* osd: randomize scrub times (#10973 Kefu Chai)
* osd: refactor scrub and digest recording (Sage Weil)
* osd: refuse first write to EC object at non-zero offset (Jianpeng Ma)
* osd: stripe over small xattrs to fit in XFS's 255 byte inline limit (Sage Weil, Ning Yao)
* osd: sync object_map on syncfs (Samuel Just)
* osd: take excl lock of op is rw (Samuel Just)
* osd: WBThrottle cleanups (Jianpeng Ma)
* pycephfs: many fixes for bindings (Haomai Wang)
* rados: bench: add --no-verify option to improve performance (Piotr Dalek)
* rados: misc bench fixes (Dmitry Yatsushkevich)
* rbd: add disk usage tool (#7746 Jason Dillaman)
* rgw: alwasy check if token is expired (#11367 Anton Aksola, Riku Lehto)
* rgw: conversion tool to repair broken multipart objects (#12079 Yehuda Sadeh)
* rgw: do not enclose bucket header in quotes (#11860 Wido den Hollander)
* rgw: error out if frontend did not send all data (#11851 Yehuda Sadeh)
* rgw: fix assignment of copy obj attributes (#11563 Yehuda Sadeh)
* rgw: fix reset_loc (#11974 Yehuda Sadeh)
* rgw: improve content-length env var handling (#11419 Robin H. Johnson)
* rgw: only scan for objects not in a namespace (#11984 Yehuda Sadeh)
* rgw: remove trailing :port from HTTP_HOST header (Sage Weil)
* rgw: shard work over multiple librados instances (Pavan Rallabhandi)
* rgw: swift: enforce Content-Type in response (#12157 Radoslaw Zarzynski)
* rgw: use attrs from source bucket on copy (#11639 Javier M. Mellid)
* rocksdb: pass options as single string (Xiaoxi Chen)
* rpm: many spec file fixes (Owen Synge, Ken Dreyer)
* tests: fixes for rbd xstests (Douglas Fuller)
* tests: fix tiering health checks (Loic Dachary)
* tests for low-level performance (Haomai Wang)
* tests: many ec non-regression improvements (Loic Dachary)
* tests: many many ec test improvements (Loic Dachary)
* upstart: throttle restarts (#11798 Sage Weil, Greg Farnum)


v9.0.1
======

This development release is delayed a bit due to tooling changes in the build
environment.  As a result the next one (v9.0.2) will have a bit more work than
is usual.

Highlights here include lots of RGW Swift fixes, RBD feature work
surrounding the new object map feature, more CephFS snapshot fixes,
and a few important CRUSH fixes.

Notable Changes
---------------

* auth: cache/reuse crypto lib key objects, optimize msg signature check (Sage Weil)
* build: allow tcmalloc-minimal (Thorsten Behrens)
* build: do not build ceph-dencoder with tcmalloc (#10691 Boris Ranto)
* build: fix pg ref disabling (William A. Kennington III)
* build: install-deps.sh improvements (Loic Dachary)
* build: misc fixes (Boris Ranto, Ken Dreyer, Owen Synge)
* ceph-authtool: fix return code on error (Gerhard Muntingh)
* ceph-disk: fix zap sgdisk invocation (Owen Synge, Thorsten Behrens)
* ceph-disk: pass --cluster arg on prepare subcommand (Kefu Chai)
* ceph-fuse, libcephfs: drop inode when rmdir finishes (#11339 Yan, Zheng)
* ceph-fuse,libcephfs: fix uninline (#11356 Yan, Zheng)
* ceph-monstore-tool: fix store-copy (Huangjun)
* common: add perf counter descriptions (Alyona Kiseleva)
* common: fix throttle max change (Henry Chang)
* crush: fix crash from invalid 'take' argument (#11602 Shiva Rkreddy, Sage Weil)
* crush: fix divide-by-2 in straw2 (#11357 Yann Dupont, Sage Weil)
* deb: fix rest-bench-dbg and ceph-test-dbg dependendies (Ken Dreyer)
* doc: document region hostnames (Robin H. Johnson)
* doc: update release schedule docs (Loic Dachary)
* init-radosgw: run radosgw as root (#11453 Ken Dreyer)
* librados: fadvise flags per op (Jianpeng Ma)
* librbd: allow additional metadata to be stored with the image (Haomai Wang)
* librbd: better handling for dup flatten requests (#11370 Jason Dillaman)
* librbd: cancel in-flight ops on watch error (#11363 Jason Dillaman)
* librbd: default new images to format 2 (#11348 Jason Dillaman)
* librbd: fast diff implementation that leverages object map (Jason Dillaman)
* librbd: fix snapshot creation when other snap is active (#11475 Jason Dillaman)
* librbd: new diff_iterate2 API (Jason Dillaman)
* librbd: object map rebuild support (Jason Dillaman)
* logrotate.d: prefer service over invoke-rc.d (#11330 Win Hierman, Sage Weil)
* mds: avoid getting stuck in XLOCKDONE (#11254 Yan, Zheng)
* mds: fix integer truncateion on large client ids (Henry Chang)
* mds: many snapshot and stray fixes (Yan, Zheng)
* mds: persist completed_requests reliably (#11048 John Spray)
* mds: separate safe_pos in Journaler (#10368 John Spray)
* mds: snapshot rename support (#3645 Yan, Zheng)
* mds: warn when clients fail to advance oldest_client_tid (#10657 Yan, Zheng)
* misc cleanups and fixes (Danny Al-Gaaf)
* mon: fix average utilization calc for 'osd df' (Mykola Golub)
* mon: fix variance calc in 'osd df' (Sage Weil)
* mon: improve callout to crushtool (Mykola Golub)
* mon: prevent bucket deletion when referenced by a crush rule (#11602 Sage Weil)
* mon: prime pg_temp when CRUSH map changes (Sage Weil)
* monclient: flush_log (John Spray)
* msgr: async: many many fixes (Haomai Wang)
* msgr: simple: fix clear_pipe (#11381 Haomai Wang)
* osd: add latency perf counters for tier operations (Xinze Chi)
* osd: avoid multiple hit set insertions (Zhiqiang Wang)
* osd: break PG removal into multiple iterations (#10198 Guang Yang)
* osd: check scrub state when handling map (Jianpeng Ma)
* osd: fix endless repair when object is unrecoverable (Jianpeng Ma, Kefu Chai)
* osd: fix pg resurrection (#11429 Samuel Just)
* osd: ignore non-existent osds in unfound calc (#10976 Mykola Golub)
* osd: increase default max open files (Owen Synge)
* osd: prepopulate needs_recovery_map when only one peer has missing (#9558 Guang Yang)
* osd: relax reply order on proxy read (#11211 Zhiqiang Wang)
* osd: skip promotion for flush/evict op (Zhiqiang Wang)
* osd: write journal header on clean shutdown (Xinze Chi)
* qa: run-make-check.sh script (Loic Dachary)
* rados bench: misc fixes (Dmitry Yatsushkevich)
* rados: fix error message on failed pool removal (Wido den Hollander)
* radosgw-admin: add 'bucket check' function to repair bucket index (Yehuda Sadeh)
* rbd: allow unmapping by spec (Ilya Dryomov)
* rbd: deprecate --new-format option (Jason Dillman)
* rgw: do not set content-type if length is 0 (#11091 Orit Wasserman)
* rgw: don't use end_marker for namespaced object listing (#11437 Yehuda Sadeh)
* rgw: fail if parts not specified on multipart upload (#11435 Yehuda Sadeh)
* rgw: fix GET on swift account when limit == 0 (#10683 Radoslaw Zarzynski)
* rgw: fix broken stats in container listing (#11285 Radoslaw Zarzynski)
* rgw: fix bug in domain/subdomain splitting (Robin H. Johnson)
* rgw: fix civetweb max threads (#10243 Yehuda Sadeh)
* rgw: fix copy metadata, support X-Copied-From for swift (#10663 Radoslaw Zarzynski)
* rgw: fix locator for objects starting with _ (#11442 Yehuda Sadeh)
* rgw: fix mulitipart upload in retry path (#11604 Yehuda Sadeh)
* rgw: fix quota enforcement on POST (#11323 Sergey Arkhipov)
* rgw: fix return code on missing upload (#11436 Yehuda Sadeh)
* rgw: force content type header on responses with no body (#11438 Orit Wasserman)
* rgw: generate new object tag when setting attrs (#11256 Yehuda Sadeh)
* rgw: issue aio for first chunk before flush cached data (#11322 Guang Yang)
* rgw: make read user buckets backward compat (#10683 Radoslaw Zarzynski)
* rgw: merge manifests properly with prefix override (#11622 Yehuda Sadeh)
* rgw: return 412 on bad limit when listing buckets (#11613 Yehuda Sadeh)
* rgw: send ETag, Last-Modified for swift (#11087 Radoslaw Zarzynski)
* rgw: set content length on container GET, PUT, DELETE, HEAD (#10971, #11036 Radoslaw Zarzynski)
* rgw: support end marker on swift container GET (#10682 Radoslaw Zarzynski)
* rgw: swift: fix account listing (#11501 Radoslaw Zarzynski)
* rgw: swift: set content-length on keystone tokens (#11473 Herv Rousseau)
* rgw: use correct oid for gc chains (#11447 Yehuda Sadeh)
* rgw: use unique request id for civetweb (#10295 Orit Wasserman)
* rocksdb, leveldb: fix compact_on_mount (Xiaoxi Chen)
* rocksdb: add perf counters for get/put latency (Xinxin Shu)
* rpm: add suse firewall files (Tim Serong)
* rpm: misc systemd and suse fixes (Owen Synge, Nathan Cutler)



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

