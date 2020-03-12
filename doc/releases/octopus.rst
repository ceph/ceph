v15.1.0 Octopus
===============

.. note: This is a release candidate and not (yet) intended for production use.

These are draft notes for the upcoming Octopus release.

Major Changes from Nautilus
---------------------------

- *General*:
  
  * A new deployment tool called **cephadm** has been introduced that
    integrates Ceph daemon deployment and management via containers
    into the orchestration layer.  For more information see
    :ref:`cephadm-bootstrap`.
  * Health alerts can now be muted, either temporarily or permanently.
  * A simple 'alerts' capability has been introduced to send email
    health alerts for clusters deployed without the benefit of an
    existing external monitoring infrastructure.
  * Health alerts are now raised for recent Ceph daemons crashes.

|

- *Dashboard*:

  The :ref:`mgr-dashboard` has gained a lot of new features and functionality:

|

  * UI Enhancements

    - New vertical navigation bar
    - New unified sidebar: better background task and events notification
    - Shows all progress mgr module notifications
    - Multi-select on tables to perform bulk operations

|

  * Dashboard user account security enhancements

    - Disabling/enabling existing user accounts
    - Clone an existing user role
    - Users can change their own password
    - Configurable password policies: Minimum password complexity/length
      requirements
    - Configurable password expiration
    - Change password after first login

|
|
  New and enhanced management of Ceph features/services:


  * OSD/device management

    - List all disks associated with an OSD
    - Add support for blinking enclosure LEDs via the orchestrator
    - List all hosts known by the orchestrator
    - List all disks and their properties attached to a node
    - Display disk health information (health prediction and SMART data)
    - Deploy new OSDs on new disks/hosts
    - Display and allow sorting by an OSD's default device class in the OSD
      table
    - Explicitly set/change the device class of an OSD, display and sort OSDs by
      device class

|

  * Pool management

    - Viewing and setting pool quotas
    - Define and change per-pool PG autoscaling mode

|

  * RGW management enhancements

    - Enable bucket versioning
    - Enable MFA support
    - Select placement target on bucket creation

|

  * CephFS management enhancements

    - CephFS client eviction
    - CephFS snapshot management
    - CephFS quota management
    - Browse CephFS directory

|

  * iSCSI management enhancements

    - Show iSCSI GW status on landing page
    - Prevent deletion of IQNs with open sessions
    - Display iSCSI "logged in" info
     
|

  * Prometheus alert management

    - List configured Prometheus alerts

|

- *RADOS*:
  
  * Objects can now be brought in sync during recovery by copying only
    the modified portion of the object, reducing tail latencies during
    recovery.
  * The PG autoscaler feature introduced in Nautilus is enabled for
    new pools by default, allowing new clusters to autotune *pg num*
    without any user intervention.  The default values for new pools
    and RGW/CephFS metadata pools have also been adjusted to perform
    well for most users.
  * BlueStore has received several improvements and performance
    updates, including improved accounting for "omap" (key/value)
    object data by pool, improved cache memory management, and a
    reduced allocation unit size for SSD devices.  (Note that by
    default, the first time each OSD starts after upgrading to octopus
    it will trigger a conversion that may take from a few minutes to a
    few hours, depending on the amount of stored "omap" data.)
  * Snapshot trimming metadata is now managed in a more efficient and
    scalable fashion.

|

- *RBD* block storage:
  
  * Clone operations now preserve the sparseness of the underlying RBD image.
  * The trash feature has been improved to (optionally) automatically
    move old parent images to the trash when their children are all
    deleted or flattened.
  * The ``rbd-nbd`` tool has been improved to use more modern kernel interfaces.
  * Caching has been improved to be more efficient and performant.

|


- *RGW* object storage:
  
  * Multi-site replication can now be managed on a per-bucket basis (EXPERIMENTAL).
  * WORM?
  * bucket tagging?

|

- *CephFS* distributed file system:
  
  * Inline data support in CephFS has been deprecated and will likely be
    removed in a future release.
  * MDS daemons can now be assigned to manage a particular file system via the
    new ``mds_join_fs`` option.
  * MDS now aggressively asks idle clients to trim caps which improves stability
    when file system load changes.
  * The mgr volumes plugin has received numerous improvements to support CephFS
    via CSI, including snapshots and cloning.
  * cephfs-shell has had numerous incremental improvements and bug fixes.

|


Upgrading from Mimic or Nautilus
--------------------------------

Notes
~~~~~

* You can monitor the progress of your upgrade at each stage with the
  ``ceph versions`` command, which will tell you what ceph version(s) are
  running for each type of daemon.

Instructions
~~~~~~~~~~~~

#. Make sure your cluster is stable and healthy (no down or
   recovering OSDs).  (Optional, but recommended.)

#. Set the ``noout`` flag for the duration of the upgrade. (Optional,
   but recommended.)::

     # ceph osd set noout

#. Upgrade monitors by installing the new packages and restarting the
   monitor daemons.  For example, on each monitor host,::

     # systemctl restart ceph-mon.target

   Once all monitors are up, verify that the monitor upgrade is
   complete by looking for the ``octopus`` string in the mon
   map.  The command::

     # ceph mon dump | grep min_mon_release

   should report::

     min_mon_release 15 (nautilus)

   If it doesn't, that implies that one or more monitors hasn't been
   upgraded and restarted and/or the quorum does not include all monitors.

#. Upgrade ``ceph-mgr`` daemons by installing the new packages and
   restarting all manager daemons.  For example, on each manager host,::

     # systemctl restart ceph-mgr.target

   Verify the ``ceph-mgr`` daemons are running by checking ``ceph
   -s``::

     # ceph -s

     ...
       services:
        mon: 3 daemons, quorum foo,bar,baz
        mgr: foo(active), standbys: bar, baz
     ...

#. Upgrade all OSDs by installing the new packages and restarting the
   ceph-osd daemons on all OSD hosts::

     # systemctl restart ceph-osd.target

   Note that the first time each OSD starts, it will do a format
   conversion to improve the accounting for "omap" data.  This may
   take a few minutes to as much as a few hours (for an HDD with lots
   of omap data).  You can disable this automatic conversion with::

     # ceph config set osd bluestore_fsck_quick_fix_on_mount false

   You can monitor the progress of the OSD upgrades with the
   ``ceph versions`` or ``ceph osd versions`` commands::

     # ceph osd versions
     {
        "ceph version 13.2.5 (...) mimic (stable)": 12,
        "ceph version 15.2.0 (...) octopus (stable)": 22,
     }

#. Upgrade all CephFS MDS daemons.  For each CephFS file system,

   #. Reduce the number of ranks to 1.  (Make note of the original
      number of MDS daemons first if you plan to restore it later.)::

	# ceph status
	# ceph fs set <fs_name> max_mds 1

   #. Wait for the cluster to deactivate any non-zero ranks by
      periodically checking the status::

	# ceph status

   #. Take all standby MDS daemons offline on the appropriate hosts with::

	# systemctl stop ceph-mds@<daemon_name>

   #. Confirm that only one MDS is online and is rank 0 for your FS::

	# ceph status

   #. Upgrade the last remaining MDS daemon by installing the new
      packages and restarting the daemon::

        # systemctl restart ceph-mds.target

   #. Restart all standby MDS daemons that were taken offline::

	# systemctl start ceph-mds.target

   #. Restore the original value of ``max_mds`` for the volume::

	# ceph fs set <fs_name> max_mds <original_max_mds>

#. Upgrade all radosgw daemons by upgrading packages and restarting
   daemons on all hosts::

     # systemctl restart ceph-radosgw.target

#. Complete the upgrade by disallowing pre-Octopus OSDs and enabling
   all new Octopus-only functionality::

     # ceph osd require-osd-release octopus

#. If you set ``noout`` at the beginning, be sure to clear it with::

     # ceph osd unset noout

#. Verify the cluster is healthy with ``ceph health``.

   If your CRUSH tunables are older than Hammer, Ceph will now issue a
   health warning.  If you see a health alert to that effect, you can
   revert this change with::

     ceph config set mon mon_crush_min_required_version firefly

   If Ceph does not complain, however, then we recommend you also
   switch any existing CRUSH buckets to straw2, which was added back
   in the Hammer release.  If you have any 'straw' buckets, this will
   result in a modest amount of data movement, but generally nothing
   too severe.::

     ceph osd getcrushmap -o backup-crushmap
     ceph osd crush set-all-straw-buckets-to-straw2

   If there are problems, you can easily revert with::

     ceph osd setcrushmap -i backup-crushmap

   Moving to 'straw2' buckets will unlock a few recent features, like
   the `crush-compat` :ref:`balancer <balancer>` mode added back in Luminous.


#. If you are upgrading from Mimic, or did not already do so when you
   upgraded to Nautlius, we recommened you enable the new :ref:`v2
   network protocol <msgr2>`, issue the following command::

     ceph mon enable-msgr2

   This will instruct all monitors that bind to the old default port
   6789 for the legacy v1 protocol to also bind to the new 3300 v2
   protocol port.  To see if all monitors have been updated,::

     ceph mon dump

   and verify that each monitor has both a ``v2:`` and ``v1:`` address
   listed.

#. Consider enabling the :ref:`telemetry module <telemetry>` to send
   anonymized usage statistics and crash information to the Ceph
   upstream developers.  To see what would be reported (without actually
   sending any information to anyone),::

     ceph mgr module enable telemetry
     ceph telemetry show

   If you are comfortable with the data that is reported, you can opt-in to
   automatically report the high-level cluster metadata with::

     ceph telemetry on

   For more information about the telemetry module, see :ref:`the
   documentation <telemetry>`.


Upgrading from pre-Mimic releases (like Luminous)
-------------------------------------------------

You *must* first upgrade to Mimic (13.2.z) or Nautilus (14.2.z) before
upgrading to Octopus.


Upgrade compatibility notes
---------------------------

* The RGW "num_rados_handles" has been removed.
  If you were using a value of "num_rados_handles" greater than 1
  multiply your current "objecter_inflight_ops" and
  "objecter_inflight_op_bytes" paramaeters by the old
  "num_rados_handles" to get the same throttle behavior.

* Ceph now packages python bindings for python3.6 instead of
  python3.4, because python3 in EL7/EL8 is now using python3.6
  as the native python3. see the `announcement <https://lists.fedoraproject.org/archives/list/epel-announce@lists.fedoraproject.org/message/EGUMKAIMPK2UD5VSHXM53BH2MBDGDWMO/>_`
  for more details on the background of this change.

* librbd now uses a write-around cache policy be default,
  replacing the previous write-back cache policy default.
  This cache policy allows librbd to immediately complete
  write IOs while they are still in-flight to the OSDs.
  Subsequent flush requests will ensure all in-flight
  write IOs are completed prior to completing. The
  librbd cache policy can be controlled via a new
  "rbd_cache_policy" configuration option.

* librbd now includes a simple IO scheduler which attempts to
  batch together multiple IOs against the same backing RBD
  data block object. The librbd IO scheduler policy can be
  controlled via a new "rbd_io_scheduler" configuration
  option.

* RGW: radosgw-admin introduces two subcommands that allow the
  managing of expire-stale objects that might be left behind after a
  bucket reshard in earlier versions of RGW. One subcommand lists such
  objects and the other deletes them. Read the troubleshooting section
  of the dynamic resharding docs for details.

* RGW: Bucket naming restrictions have changed and likely to cause
  InvalidBucketName errors. We recommend to set ``rgw_relaxed_s3_bucket_names``
  option to true as a workaround.

* In the Zabbix Mgr Module there was a typo in the key being send
  to Zabbix for PGs in backfill_wait state. The key that was sent
  was 'wait_backfill' and the correct name is 'backfill_wait'.
  Update your Zabbix template accordingly so that it accepts the
  new key being send to Zabbix.

* zabbix plugin for ceph manager now includes osd and pool
  discovery. Update of zabbix_template.xml is needed
  to receive per-pool (read/write throughput, diskspace usage)
  and per-osd (latency, status, pgs) statistics

* The format of all date + time stamps has been modified to fully
  conform to ISO 8601.  The old format (``YYYY-MM-DD
  HH:MM:SS.ssssss``) excluded the ``T`` separator between the date and
  time and was rendered using the local time zone without any explicit
  indication.  The new format includes the separator as well as a
  ``+nnnn`` or ``-nnnn`` suffix to indicate the time zone, or a ``Z``
  suffix if the time is UTC.  For example,
  ``2019-04-26T18:40:06.225953+0100``.

  Any code or scripts that was previously parsing date and/or time
  values from the JSON or XML structure CLI output should be checked
  to ensure it can handle ISO 8601 conformant values.  Any code
  parsing date or time values from the unstructured human-readable
  output should be modified to parse the structured output instead, as
  the human-readable output may change without notice.

* The ``bluestore_no_per_pool_stats_tolerance`` config option has been
  replaced with ``bluestore_fsck_error_on_no_per_pool_stats``
  (default: false).  The overall default behavior has not changed:
  fsck will warn but not fail on legacy stores, and repair will
  convert to per-pool stats.

* The disaster-recovery related 'ceph mon sync force' command has been
  replaced with 'ceph daemon <...> sync_force'.

* The ``osd_recovery_max_active`` option now has
  ``osd_recovery_max_active_hdd`` and ``osd_recovery_max_active_ssd``
  variants, each with different default values for HDD and SSD-backed
  OSDs, respectively.  By default ``osd_recovery_max_active`` now
  defaults to zero, which means that the OSD will conditionally use
  the HDD or SSD option values.  Administrators who have customized
  this value may want to consider whether they have set this to a
  value similar to the new defaults (3 for HDDs and 10 for SSDs) and,
  if so, remove the option from their configuration entirely.

* monitors now have a `ceph osd info` command that will provide information
  on all osds, or provided osds, thus simplifying the process of having to
  parse `osd dump` for the same information.

* The structured output of ``ceph status`` or ``ceph -s`` is now more
  concise, particularly the `mgrmap` and `monmap` sections, and the
  structure of the `osdmap` section has been cleaned up.

* A health warning is now generated if the average osd heartbeat ping
  time exceeds a configurable threshold for any of the intervals
  computed.  The OSD computes 1 minute, 5 minute and 15 minute
  intervals with average, minimum and maximum values.  New
  configuration option ``mon_warn_on_slow_ping_ratio`` specifies a
  percentage of ``osd_heartbeat_grace`` to determine the threshold.  A
  value of zero disables the warning.  New configuration option
  ``mon_warn_on_slow_ping_time`` specified in milliseconds over-rides
  the computed value, causes a warning when OSD heartbeat pings take
  longer than the specified amount.  New admin command ``ceph daemon
  mgr.# dump_osd_network [threshold]`` command will list all
  connections with a ping time longer than the specified threshold or
  value determined by the config options, for the average for any of
  the 3 intervals.  New admin command ``ceph daemon osd.#
  dump_osd_network [threshold]`` will do the same but only including
  heartbeats initiated by the specified OSD.

* Inline data support for CephFS has been deprecated. When setting the flag,
  users will see a warning to that effect, and enabling it now requires the
  ``--yes-i-really-really-mean-it`` flag. If the MDS is started on a
  filesystem that has it enabled, a health warning is generated. Support for
  this feature will be removed in a future release.

* ``ceph {set,unset} full`` is not supported anymore. We have been using
  ``full`` and ``nearfull`` flags in OSD map for tracking the fullness status
  of a cluster back since the Hammer release, if the OSD map is marked ``full``
  all write operations will be blocked until this flag is removed. In the
  Infernalis release and Linux kernel 4.7 client, we introduced the per-pool
  full/nearfull flags to track the status for a finer-grained control, so the
  clients will hold the write operations if either the cluster-wide ``full``
  flag or the per-pool ``full`` flag is set. This was a compromise, as we
  needed to support the cluster with and without per-pool ``full`` flags
  support. But this practically defeated the purpose of introducing the
  per-pool flags. So, in the Mimic release, the new flags finally took the
  place of their cluster-wide counterparts, as the monitor started removing
  these two flags from OSD map. So the clients of Infernalis and up can benefit
  from this change, as they won't be blocked by the full pools which they are
  not writing to. In this release, ``ceph {set,unset} full`` is now considered
  as an invalid command. And the clients will continue honoring both the
  cluster-wide and per-pool flags to be backward comaptible with pre-infernalis
  clusters.

* The telemetry module now reports more information.

  First, there is a new 'device' channel, enabled by default, that
  will report anonymized hard disk and SSD health metrics to
  telemetry.ceph.com in order to build and improve device failure
  prediction algorithms.  If you are not comfortable sharing device
  metrics, you can disable that channel first before re-opting-in::

    ceph config set mgr mgr/telemetry/channel_device false

  Second, we now report more information about CephFS file systems,
  including:

    - how many MDS daemons (in total and per file system)
    - which features are (or have been) enabled
    - how many data pools
    - approximate file system age (year + month of creation)
    - how many files, bytes, and snapshots
    - how much metadata is being cached

  We have also added:

    - which Ceph release the monitors are running
    - whether msgr v1 or v2 addresses are used for the monitors
    - whether IPv4 or IPv6 addresses are used for the monitors
    - whether RADOS cache tiering is enabled (and which mode)
    - whether pools are replicated or erasure coded, and
      which erasure code profile plugin and parameters are in use
    - how many hosts are in the cluster, and how many hosts have each type of daemon
    - whether a separate OSD cluster network is being used
    - how many RBD pools and images are in the cluster, and how many pools have RBD mirroring enabled
    - how many RGW daemons, zones, and zonegroups are present; which RGW frontends are in use
    - aggregate stats about the CRUSH map, like which algorithms are used, how
      big buckets are, how many rules are defined, and what tunables are in
      use

  If you had telemetry enabled, you will need to re-opt-in with::

    ceph telemetry on

  You can view exactly what information will be reported first with::

    ceph telemetry show        # see everything
    ceph telemetry show basic  # basic cluster info (including all of the new info)

* Following invalid settings now are not tolerated anymore
  for the command `ceph osd erasure-code-profile set xxx`.
  * invalid `m` for "reed_sol_r6_op" erasure technique
  * invalid `m` and invalid `w` for "liber8tion" erasure technique

* New OSD daemon command dump_recovery_reservations which reveals the
  recovery locks held (in_progress) and waiting in priority queues.

* New OSD daemon command dump_scrub_reservations which reveals the
  scrub reservations that are held for local (primary) and remote (replica) PGs.

* Previously, ``ceph tell mgr ...`` could be used to call commands
  implemented by mgr modules.  This is no longer supported.  Since
  luminous, using ``tell`` has not been necessary: those same commands
  are also accessible without the ``tell mgr`` portion (e.g., ``ceph
  tell mgr influx foo`` is the same as ``ceph influx foo``.  ``ceph
  tell mgr ...`` will now call admin commands--the same set of
  commands accessible via ``ceph daemon ...`` when you are logged into
  the appropriate host.

* The ``ceph tell`` and ``ceph daemon`` commands have been unified,
  such that all such commands are accessible via either interface.
  Note that ceph-mgr tell commands are accessible via either ``ceph
  tell mgr ...`` or ``ceph tell mgr.<id> ...``, and it is only
  possible to send tell commands to the active daemon (the standbys do
  not accept incoming connections over the network).

* Ceph will now issue a health warning if a RADOS pool as a ``pg_num``
  value that is not a power of two.  This can be fixed by adjusting
  the pool to a nearby power of two::

    ceph osd pool set <pool-name> pg_num <new-pg-num>

  Alternatively, the warning can be silenced with::

    ceph config set global mon_warn_on_pool_pg_num_not_power_of_two false

* The format of MDSs in `ceph fs dump` has changed.

* The ``mds_cache_size`` config option is completely removed. Since luminous,
  the ``mds_cache_memory_limit`` config option has been preferred to configure
  the MDS's cache limits.

* The ``pg_autoscale_mode`` is now set to ``on`` by default for newly
  created pools, which means that Ceph will automatically manage the
  number of PGs.  To change this behavior, or to learn more about PG
  autoscaling, see :ref:`pg-autoscaler`.  Note that existing pools in
  upgraded clusters will still be set to ``warn`` by default.

* The ``upmap_max_iterations`` config option of mgr/balancer has been
  renamed to ``upmap_max_optimizations`` to better match its behaviour.

* ``mClockClientQueue`` and ``mClockClassQueue`` OpQueue
  implementations have been removed in favor of of a single
  ``mClockScheduler`` implementation of a simpler OSD interface.
  Accordingly, the ``osd_op_queue_mclock*`` family of config options
  has been removed in favor of the ``osd_mclock_scheduler*`` family
  of options.

* The config subsystem now searches dot ('.') delineated prefixes for
  options.  That means for an entity like ``client.foo.bar``, it's
  overall configuration will be a combination of the global options,
  ``client``, ``client.foo``, and ``client.foo.bar``.  Previously,
  only global, ``client``, and ``client.foo.bar`` options would apply.
  This change may affect the configuration for clients that include a
  ``.`` in their name.

  Note that this only applies to configuration options in the
