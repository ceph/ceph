===============
 Release Notes
===============

v0.53 (not yet released)
------------------------

Notable changes
~~~~~~~~~~~~~~~

* ceph tool: the second (redundant) osd.NNN argument to 'ceph osd crush set NNN osd.NNN <weight> <loc ...>' is now optional, and both the NNN and osd.NNN forms are now accepted for the first argument.  This makes usage simpler without breaking existing scripts.


v0.48.1 "argonaut"
------------------


Upgrading
~~~~~~~~~

* The radosgw usage trim function was effectively broken in v0.48.  Earlier it would remove more usage data than what was requested.  This is fixed in v0.48.1, but the fix is incompatible.  The v0.48 radosgw-admin tool cannot be used to initiate the trimming; please use the v0.48.1 version.

* v0.48.1 now explicitly indicates support for the CRUSH_TUNABLES feature.  No other version of Ceph requires this, yet, but future versions will when the tunables are adjusted from their historical defaults.

* There are no other compatibility changes between v0.48.1 and v0.48.

Notable changes
~~~~~~~~~~~~~~~

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
----------------

Upgrading
~~~~~~~~~

* This release includes a disk format upgrade.  Each ceph-osd daemon, upon startup, will migrate its locally stored data to the new format.  This process can take a while (for large object counts, even hours), especially on non-btrfs file systems.  

* To keep the cluster available while the upgrade is in progress, we recommend you upgrade a storage node or rack at a time, and wait for the cluster to recover each time.  To prevent the cluster from moving data around in response to the OSD daemons being down for minutes or hours, you may want to::

    ceph osd set noout

  This will prevent the cluster from marking down OSDs as "out" and re-replicating the data elsewhere. If you do this, be sure to clear the flag when the upgrade is complete::

    ceph osd unset noout

* There is a encoding format change internal to the monitor cluster. The monitor daemons are careful to switch to the new format only when all members of the quorum support it.  However, that means that a partial quorum with new code may move to the new format, and a recovering monitor running old code will be unable to join (it will crash).  If this occurs, simply upgrading the remaining monitor will resolve the problem.

* The ceph tool's -s and -w commands from previous versions are incompatible with this version. Upgrade your client tools at the same time you upgrade the monitors if you rely on those commands.

* It is not possible to downgrade from v0.48 to a previous version.

Notable changes
~~~~~~~~~~~~~~~

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



