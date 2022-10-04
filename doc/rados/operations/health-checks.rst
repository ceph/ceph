.. _health-checks:

===============
 Health checks 
===============

Overview
========

There is a finite set of health messages that a Ceph cluster can raise. These
messages are known as *health checks*. Each health check has a unique
identifier.

The identifier is a terse human-readable string -- that is, the identifier is
readable in much the same way as a typical variable name. It is intended to
enable tools (for example, UIs) to make sense of health checks and present them
in a way that reflects their meaning.

This page lists the health checks that are raised by the monitor and manager
daemons. In addition to these, you might see health checks that originate
from MDS daemons (see :ref:`cephfs-health-messages`), and health checks
that are defined by ``ceph-mgr`` python modules.

Definitions
===========

Monitor
-------

DAEMON_OLD_VERSION
__________________

Warn if one or more old versions of Ceph are running on any daemons.  A health
check is raised if multiple versions are detected.  This condition must exist
for a period of time greater than ``mon_warn_older_version_delay`` (set to one
week by default) in order for the health check to be raised. This allows most
upgrades to proceed without the occurrence of a false warning. If the upgrade
is paused for an extended time period, ``health mute`` can be used by running
``ceph health mute DAEMON_OLD_VERSION --sticky``. Be sure, however, to run
``ceph health unmute DAEMON_OLD_VERSION`` after the upgrade has finished.

MON_DOWN
________

One or more monitor daemons are currently down. The cluster requires a majority
(more than one-half) of the monitors to be available. When one or more monitors
are down, clients might have a harder time forming their initial connection to
the cluster, as they might need to try more addresses before they reach an
operating monitor.

The down monitor daemon should be restarted as soon as possible to reduce the
risk of a subsequent monitor failure leading to a service outage.

MON_CLOCK_SKEW
______________

The clocks on the hosts running the ceph-mon monitor daemons are not
well-synchronized. This health check is raised if the cluster detects a clock
skew greater than ``mon_clock_drift_allowed``.

This issue is best resolved by synchronizing the clocks by using a tool like
``ntpd`` or ``chrony``.

If it is impractical to keep the clocks closely synchronized, the
``mon_clock_drift_allowed`` threshold can also be increased. However, this
value must stay significantly below the ``mon_lease`` interval in order for the
monitor cluster to function properly.

MON_MSGR2_NOT_ENABLED
_____________________

The `ms_bind_msgr2` option is enabled but one or more monitors are
not configured to bind to a v2 port in the cluster's monmap. This
means that features specific to the msgr2 protocol (for example, encryption)
are unavailable on some or all connections.

In most cases this can be corrected by running the following command:

.. prompt:: bash $

   ceph mon enable-msgr2

After this command is run, any monitor configured to listen on the old default
port (6789) will continue to listen for v1 connections on 6789 and begin to
listen for v2 connections on the new default port 3300.

If a monitor is configured to listen for v1 connections on a non-standard port
(that is, a port other than 6789), then the monmap will need to be modified
manually.


MON_DISK_LOW
____________

One or more monitors are low on disk space. This health check is raised if the
percentage of available space on the file system used by the monitor database
(normally ``/var/lib/ceph/mon``) drops below the percentage value
``mon_data_avail_warn`` (default: 30%).

This alert might indicate that some other process or user on the system is
filling up the file system used by the monitor. It might also
indicate that the monitor database is too large (see ``MON_DISK_BIG``
below).

If space cannot be freed, the monitor's data directory might need to be
moved to another storage device or file system (this relocation process must be carried out while the monitor
daemon is not running).


MON_DISK_CRIT
_____________

One or more monitors are critically low on disk space. This health check is raised if the
percentage of available space on the file system used by the monitor database
(normally ``/var/lib/ceph/mon``) drops below the percentage value
``mon_data_avail_crit`` (default: 5%). See ``MON_DISK_LOW``, above.

MON_DISK_BIG
____________

The database size for one or more monitors is very large. This health check is
raised if the size of the monitor database is larger than
``mon_data_size_warn`` (default: 15 GiB).

A large database is unusual, but does not necessarily indicate a problem.
Monitor databases might grow in size when there are placement groups that have
not reached an ``active+clean`` state in a long time.

This alert might also indicate that the monitor's database is not properly
compacting, an issue that has been observed with some older versions of leveldb
and rocksdb. Forcing a compaction with ``ceph daemon mon.<id> compact`` might
shrink the database's on-disk size.

This alert might also indicate that the monitor has a bug that prevents it from
pruning the cluster metadata that it stores. If the problem persists, please
report a bug.

To adjust the warning threshold, run the following command:

.. prompt:: bash $

   ceph config set global mon_data_size_warn <size>


AUTH_INSECURE_GLOBAL_ID_RECLAIM
_______________________________

One or more clients or daemons that are connected to the cluster are not
securely reclaiming their ``global_id`` (a unique number that identifies each
entity in the cluster) when reconnecting to a monitor. The client is being
permitted to connect anyway because the
``auth_allow_insecure_global_id_reclaim`` option is set to ``true`` (which may
be necessary until all Ceph clients have been upgraded) and because the
``auth_expose_insecure_global_id_reclaim`` option is set to ``true`` (which
allows monitors to detect clients with "insecure reclaim" sooner by forcing
those clients to reconnect immediately after their initial authentication).

To identify which client(s) are using unpatched Ceph client code, run the
following command:

.. prompt:: bash $

   ceph health detail

If you collect a dump of the clients that are connected to an individual
monitor and examine the ``global_id_status`` field in the output of the dump,
you can see the ``global_id`` reclaim behavior of those clients. Here
``reclaim_insecure`` means that a client is unpatched and is contributing to
this health check.  To effect a client dump, run the following command:

.. prompt:: bash $

   ceph tell mon.\* sessions

We strongly recommend that all clients in the system be upgraded to a newer
version of Ceph that correctly reclaims ``global_id`` values. After all clients
have been updated, run the following command to stop allowing insecure
reconnections:

.. prompt:: bash $

   ceph config set mon auth_allow_insecure_global_id_reclaim false

If it is impractical to upgrade all clients immediately, you can temporarily
silence this alert by running the following command:

.. prompt:: bash $

   ceph health mute AUTH_INSECURE_GLOBAL_ID_RECLAIM 1w   # 1 week

Although we do NOT recommend doing so, you can also disable this alert
indefinitely by running the following command:

.. prompt:: bash $

   ceph config set mon mon_warn_on_insecure_global_id_reclaim false

AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED
_______________________________________

Ceph is currently configured to allow clients that reconnect to monitors using
an insecure process to reclaim their previous ``global_id``. Such reclaiming is
allowed because, by default, ``auth_allow_insecure_global_id_reclaim`` is set
to ``true``. It might be necessary to leave this setting enabled while existing
Ceph clients are upgraded to newer versions of Ceph that correctly and securely
reclaim their ``global_id``.

If the ``AUTH_INSECURE_GLOBAL_ID_RECLAIM`` health check has not also been
raised and if the ``auth_expose_insecure_global_id_reclaim`` setting has not
been disabled (it is enabled by default), then there are currently no clients
connected that need to be upgraded. In that case, it is safe to disable
``insecure global_id reclaim`` by running the following command:

.. prompt:: bash $

   ceph config set mon auth_allow_insecure_global_id_reclaim false

On the other hand, if there are still clients that need to be upgraded, then
this alert can be temporarily silenced by running the following command:

.. prompt:: bash $

   ceph health mute AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED 1w   # 1 week

Although we do NOT recommend doing so, you can also disable this alert indefinitely
by running the following command:

.. prompt:: bash $

   ceph config set mon mon_warn_on_insecure_global_id_reclaim_allowed false


Manager
-------

MGR_DOWN
________

All manager daemons are currently down. The cluster should normally have at
least one running manager (``ceph-mgr``) daemon. If no manager daemon is
running, the cluster's ability to monitor itself will be compromised, and parts
of the management API will become unavailable (for example, the dashboard will
not work, and most CLI commands that report metrics or runtime state will
block). However, the cluster will still be able to perform all I/O operations
and to recover from failures.

The "down" manager daemon should be restarted as soon as possible to ensure
that the cluster can be monitored (for example, so that the ``ceph -s``
information is up to date, or so that metrics can be scraped by Prometheus).


MGR_MODULE_DEPENDENCY
_____________________

An enabled manager module is failing its dependency check. This health check
typically comes with an explanatory message from the module about the problem.

For example, a module might report that a required package is not installed: in
this case, you should install the required package and restart your manager
daemons.

This health check is applied only to enabled modules. If a module is not
enabled, you can see whether it is reporting dependency issues in the output of
`ceph module ls`.


MGR_MODULE_ERROR
________________

A manager module has experienced an unexpected error. Typically, this means
that an unhandled exception was raised from the module's `serve` function. The
human-readable description of the error might be obscurely worded if the
exception did not provide a useful description of itself.

This health check might indicate a bug: please open a Ceph bug report if you
think you have encountered a bug.

However, if you believe the error is transient, you may restart your manager
daemon(s) or use ``ceph mgr fail`` on the active daemon in order to force
failover to another daemon.

OSDs
----

OSD_DOWN
________

One or more OSDs are marked "down". The ceph-osd daemon might have been
stopped, or peer OSDs might be unable to reach the OSD over the network.
Common causes include a stopped or crashed daemon, a "down" host, or a network
outage.

Verify that the host is healthy, the daemon is started, and the network is
functioning. If the daemon has crashed, the daemon log file
(``/var/log/ceph/ceph-osd.*``) might contain debugging information.

OSD_<crush type>_DOWN
_____________________

(for example, OSD_HOST_DOWN, OSD_ROOT_DOWN)

All of the OSDs within a particular CRUSH subtree are marked "down" (for
example, all OSDs on a host).

OSD_ORPHAN
__________

An OSD is referenced in the CRUSH map hierarchy, but does not exist.

To remove the OSD from the CRUSH map hierarchy, run the following command:

.. prompt:: bash $

   ceph osd crush rm osd.<id>

OSD_OUT_OF_ORDER_FULL
_____________________

The utilization thresholds for `nearfull`, `backfillfull`, `full`, and/or
`failsafe_full` are not ascending. In particular, the following pattern is
expected: `nearfull < backfillfull`, `backfillfull < full`, and `full <
failsafe_full`.

To adjust these utilization thresholds, run the following commands:

.. prompt:: bash $

   ceph osd set-nearfull-ratio <ratio>
   ceph osd set-backfillfull-ratio <ratio>
   ceph osd set-full-ratio <ratio>


OSD_FULL
________

One or more OSDs have exceeded the `full` threshold and are preventing the
cluster from servicing writes.

To check utilization by pool, run the following command:

.. prompt:: bash $

   ceph df

To see the currently defined `full` ratio, run the following command:

.. prompt:: bash $

   ceph osd dump | grep full_ratio

A short-term workaround to restore write availability is to raise the full
threshold by a small amount. To do so, run the following command:

.. prompt:: bash $

   ceph osd set-full-ratio <ratio>

Additional OSDs should be deployed in order to add new storage to the cluster,
or existing data should be deleted in order to free up space in the cluster.

OSD_BACKFILLFULL
________________

One or more OSDs have exceeded the `backfillfull` threshold or *would* exceed
it if the currently-mapped backfills were to finish, which will prevent data
from rebalancing to this OSD. This alert is an early warning that
rebalancing might be unable to complete and that the cluster is approaching
full.

To check utilization by pool, run the following command:

.. prompt:: bash $

   ceph df

OSD_NEARFULL
____________

One or more OSDs have exceeded the `nearfull` threshold. This alert is an early
warning that the cluster is approaching full.

To check utilization by pool, run the following command:

.. prompt:: bash $

   ceph df

OSDMAP_FLAGS
____________

One or more cluster flags of interest have been set. These flags include:

* *full* - the cluster is flagged as full and cannot serve writes
* *pauserd*, *pausewr* - there are paused reads or writes
* *noup* - OSDs are not allowed to start
* *nodown* - OSD failure reports are being ignored, and that means that the
  monitors will not mark OSDs "down"
* *noin* - OSDs that were previously marked ``out`` are not being marked
  back ``in`` when they start
* *noout* - "down" OSDs are not automatically being marked ``out`` after the
  configured interval
* *nobackfill*, *norecover*, *norebalance* - recovery or data
  rebalancing is suspended
* *noscrub*, *nodeep_scrub* - scrubbing is disabled
* *notieragent* - cache-tiering activity is suspended

With the exception of *full*, these flags can be set or cleared by running the
following commands:

.. prompt:: bash $

   ceph osd set <flag>
   ceph osd unset <flag>

OSD_FLAGS
_________

One or more OSDs or CRUSH {nodes,device classes} have a flag of interest set.
These flags include:

* *noup*: these OSDs are not allowed to start
* *nodown*: failure reports for these OSDs will be ignored
* *noin*: if these OSDs were previously marked ``out`` automatically
  after a failure, they will not be marked ``in`` when they start
* *noout*: if these OSDs are "down" they will not automatically be marked
  ``out`` after the configured interval

To set and clear these flags in batch, run the following commands:

.. prompt:: bash $

   ceph osd set-group <flags> <who>
   ceph osd unset-group <flags> <who>

For example:

.. prompt:: bash $

   ceph osd set-group noup,noout osd.0 osd.1
   ceph osd unset-group noup,noout osd.0 osd.1
   ceph osd set-group noup,noout host-foo
   ceph osd unset-group noup,noout host-foo
   ceph osd set-group noup,noout class-hdd
   ceph osd unset-group noup,noout class-hdd

OLD_CRUSH_TUNABLES
__________________

The CRUSH map is using very old settings and should be updated. The oldest set
of tunables that can be used (that is, the oldest client version that can
connect to the cluster) without raising this health check is determined by the
``mon_crush_min_required_version`` config option.  For more information, see
:ref:`crush-map-tunables`.

OLD_CRUSH_STRAW_CALC_VERSION
____________________________

The CRUSH map is using an older, non-optimal method of calculating intermediate
weight values for ``straw`` buckets.

The CRUSH map should be updated to use the newer method (that is:
``straw_calc_version=1``). For more information, see :ref:`crush-map-tunables`.

CACHE_POOL_NO_HIT_SET
_____________________

One or more cache pools are not configured with a *hit set* to track
utilization. This issue prevents the tiering agent from identifying cold
objects that are to be flushed and evicted from the cache.

To configure hit sets on the cache pool, run the following commands:

.. prompt:: bash $

   ceph osd pool set <poolname> hit_set_type <type>
   ceph osd pool set <poolname> hit_set_period <period-in-seconds>
   ceph osd pool set <poolname> hit_set_count <number-of-hitsets>
   ceph osd pool set <poolname> hit_set_fpp <target-false-positive-rate>

OSD_NO_SORTBITWISE
__________________

No pre-Luminous v12.y.z OSDs are running, but the ``sortbitwise`` flag has not
been set.

The ``sortbitwise`` flag must be set in order for OSDs running Luminous v12.y.z
or newer to start. To safely set the flag, run the following command:

.. prompt:: bash $

   ceph osd set sortbitwise

OSD_FILESTORE
__________________

Warn if OSDs are running Filestore. The Filestore OSD back end has been
deprecated; the BlueStore back end has been the default object store since the
Ceph Luminous release.

The 'mclock_scheduler' is not supported for Filestore OSDs. For this reason,
the default 'osd_op_queue' is set to 'wpq' for Filestore OSDs and is enforced
even if the user attempts to change it.



.. prompt:: bash $

   ceph report | jq -c '."osd_metadata" | .[] | select(.osd_objectstore | contains("filestore")) | {id, osd_objectstore}'

**In order to upgrade to Reef or a later release, you must first migrate any
Filestore OSDs to BlueStore.**

If you are upgrading a pre-Reef release to Reef or later, but it is not
feasible to migrate Filestore OSDs to BlueStore immediately, you can
temporarily silence this alert by running the following command:

.. prompt:: bash $

   ceph health mute OSD_FILESTORE

Since this migration can take a considerable amount of time to complete, we
recommend that you begin the process well in advance of any update to Reef or
to later releases.

POOL_FULL
_________

One or more pools have reached their quota and are no longer allowing writes.

To see pool quotas and utilization, run the following command:

.. prompt:: bash $

   ceph df detail

If you opt to raise the pool quota, run the following commands:

.. prompt:: bash $

   ceph osd pool set-quota <poolname> max_objects <num-objects>
   ceph osd pool set-quota <poolname> max_bytes <num-bytes>

If not, delete some existing data to reduce utilization.

BLUEFS_SPILLOVER
________________

One or more OSDs that use the BlueStore back end have been allocated `db`
partitions (that is, storage space for metadata, normally on a faster device),
but because that space has been filled, metadata has "spilled over" onto the
slow device. This is not necessarily an error condition or even unexpected
behavior, but may result in degraded performance. If the administrator had
expected that all metadata would fit on the faster device, this alert indicates
that not enough space was provided.

To disable this alert on all OSDs, run the following command:

.. prompt:: bash $

   ceph config set osd bluestore_warn_on_bluefs_spillover false

Alternatively, to disable the alert on a specific OSD, run the following
command:

.. prompt:: bash $

   ceph config set osd.123 bluestore_warn_on_bluefs_spillover false

To secure more metadata space, you can destroy and reprovision the OSD in
question. This process involves data migration and recovery.

It might also be possible to expand the LVM logical volume that backs the `db`
storage. If the underlying LV has been expanded, you must stop the OSD daemon
and inform BlueFS of the device-size change by running the following command:

.. prompt:: bash $

   ceph-bluestore-tool bluefs-bdev-expand --path /var/lib/ceph/osd/ceph-$ID

BLUEFS_AVAILABLE_SPACE
______________________

To see how much space is free for BlueFS, run the following command:

.. prompt:: bash $

   ceph daemon osd.123 bluestore bluefs available

This will output up to three values: ``BDEV_DB free``, ``BDEV_SLOW free``, and
``available_from_bluestore``. ``BDEV_DB`` and ``BDEV_SLOW`` report the amount
of space that has been acquired by BlueFS and is now considered free. The value
``available_from_bluestore`` indicates the ability of BlueStore to relinquish
more space to BlueFS.  It is normal for this value to differ from the amount of
BlueStore free space, because the BlueFS allocation unit is typically larger
than the BlueStore allocation unit.  This means that only part of the BlueStore
free space will be available for BlueFS.

BLUEFS_LOW_SPACE
_________________

If BlueFS is running low on available free space and there is not much free
space available from BlueStore (in other words, `available_from_bluestore` has
a low value), consider reducing the BlueFS allocation unit size. To simulate
available space when the allocation unit is different, run the following
command: 

.. prompt:: bash $

   ceph daemon osd.123 bluestore bluefs available <alloc-unit-size>

BLUESTORE_FRAGMENTATION
_______________________

As BlueStore operates, the free space on the underlying storage will become
fragmented.  This is normal and unavoidable, but excessive fragmentation causes
slowdown.  To inspect BlueStore fragmentation, run the following command:

.. prompt:: bash $

   ceph daemon osd.123 bluestore allocator score block

The fragmentation score is given in a [0-1] range.
[0.0 .. 0.4] tiny fragmentation
[0.4 .. 0.7] small, acceptable fragmentation
[0.7 .. 0.9] considerable, but safe fragmentation
[0.9 .. 1.0] severe fragmentation, might impact BlueFS's ability to get space from BlueStore

To see a detailed report of free fragments, run the following command:

.. prompt:: bash $

   ceph daemon osd.123 bluestore allocator dump block

For OSD processes that are not currently running, fragmentation can be
inspected with `ceph-bluestore-tool`. To see the fragmentation score, run the
following command:

.. prompt:: bash $

   ceph-bluestore-tool --path /var/lib/ceph/osd/ceph-123 --allocator block free-score

To dump detailed free chunks, run the following command:

.. prompt:: bash $

   ceph-bluestore-tool --path /var/lib/ceph/osd/ceph-123 --allocator block free-dump

BLUESTORE_LEGACY_STATFS
_______________________

One or more OSDs have BlueStore volumes that were created prior to the
Nautilus release. (In Nautilus, BlueStore tracks its internal usage
statistics on a granular, per-pool basis.)

If *all* OSDs
are older than Nautilus, this means that the per-pool metrics are
simply unavailable. But if there is a mixture of pre-Nautilus and
post-Nautilus OSDs, the cluster usage statistics reported by ``ceph
df`` will be inaccurate.

The old OSDs can be updated to use the new usage-tracking scheme by stopping
each OSD, running a repair operation, and then restarting the OSD. For example,
to update ``osd.123``, run the following commands:

.. prompt:: bash $

   systemctl stop ceph-osd@123
   ceph-bluestore-tool repair --path /var/lib/ceph/osd/ceph-123
   systemctl start ceph-osd@123

To disable this alert, run the following command:

.. prompt:: bash $

   ceph config set global bluestore_warn_on_legacy_statfs false

BLUESTORE_NO_PER_POOL_OMAP
__________________________

One or more OSDs have volumes that were created prior to the Octopus release.
(In Octopus and later releases, BlueStore tracks omap space utilization by
pool.)

If there are any BlueStore OSDs that do not have the new tracking enabled, the
cluster will report an approximate value for per-pool omap usage based on the
most recent deep scrub.

The OSDs can be updated to track by pool by stopping each OSD, running a repair
operation, and then restarting the OSD. For example, to update ``osd.123``, run
the following commands:

.. prompt:: bash $

   systemctl stop ceph-osd@123
   ceph-bluestore-tool repair --path /var/lib/ceph/osd/ceph-123
   systemctl start ceph-osd@123

To disable this alert, run the following command:

.. prompt:: bash $

   ceph config set global bluestore_warn_on_no_per_pool_omap false

BLUESTORE_NO_PER_PG_OMAP
__________________________

One or more OSDs have volumes that were created prior to Pacific.  (In Pacific
and later releases Bluestore tracks omap space utilitzation by Placement Group
(PG).)

Per-PG omap allows faster PG removal when PGs migrate.

The older OSDs can be updated to track by PG by stopping each OSD, running a
repair operation, and then restarting the OSD. For example, to update
``osd.123``, run the following commands:

.. prompt:: bash $

   systemctl stop ceph-osd@123
   ceph-bluestore-tool repair --path /var/lib/ceph/osd/ceph-123
   systemctl start ceph-osd@123

To disable this alert, run the following command:

.. prompt:: bash $

   ceph config set global bluestore_warn_on_no_per_pg_omap false


BLUESTORE_DISK_SIZE_MISMATCH
____________________________

One or more BlueStore OSDs have an internal inconsistency between the size of
the physical device and the metadata that tracks its size. This inconsistency
can lead to the OSD(s) crashing in the future.

The OSDs that have this inconsistency should be destroyed and reprovisioned. Be
very careful to execute this procedure on only one OSD at a time, so as to
minimize the risk of losing any data. To execute this procedure, where ``$N``
is the OSD that has the inconsistency, run the following commands:

.. prompt:: bash $

   ceph osd out osd.$N
   while ! ceph osd safe-to-destroy osd.$N ; do sleep 1m ; done
   ceph osd destroy osd.$N
   ceph-volume lvm zap /path/to/device
   ceph-volume lvm create --osd-id $N --data /path/to/device

.. note::

   Wait for this recovery procedure to completely on one OSD before running it
   on the next.

BLUESTORE_NO_COMPRESSION
________________________

One or more OSDs is unable to load a BlueStore compression plugin.  This issue
might be caused by a broken installation, in which the ``ceph-osd`` binary does
not match the compression plugins. Or it might be caused by a recent upgrade in
which the ``ceph-osd`` daemon was not restarted.

To resolve this issue, verify that all of the packages on the host that is
running the affected OSD(s) are correctly installed and that the OSD daemon(s)
have been restarted. If the problem persists, check the OSD log for information
about the source of the problem.

BLUESTORE_SPURIOUS_READ_ERRORS
______________________________

One or more BlueStore OSDs detect spurious read errors on the main device.
BlueStore has recovered from these errors by retrying disk reads.  This alert
might indicate issues with underlying hardware, issues with the I/O subsystem,
or something similar.  In theory, such issues can cause permanent data
corruption.  Some observations on the root cause of spurious read errors can be
found here: https://tracker.ceph.com/issues/22464

This alert does not require an immediate response, but the affected host might
need additional attention: for example, upgrading the host to the latest
OS/kernel versions and implementing hardware-resource-utilization monitoring.

To disable this alert on all OSDs, run the following command:

.. prompt:: bash $

   ceph config set osd bluestore_warn_on_spurious_read_errors false

Or, to disable this alert on a specific OSD, run the following command:

.. prompt:: bash $

   ceph config set osd.123 bluestore_warn_on_spurious_read_errors false

Device health
-------------

DEVICE_HEALTH
_____________

One or more OSD devices are expected to fail soon, where the warning threshold
is determined by the ``mgr/devicehealth/warn_threshold`` config option.

Because this alert applies only to OSDs that are currently marked ``in``, the
appropriate response to this expected failure is (1) to mark the OSD ``out`` so
that data is migrated off of the OSD, and then (2) to remove the hardware from
the system. Note that this marking ``out`` is normally done automatically if
``mgr/devicehealth/self_heal`` is enabled (as determined by
``mgr/devicehealth/mark_out_threshold``).

To check device health, run the following command:

.. prompt:: bash $

   ceph device info <device-id>

Device life expectancy is set either by a prediction model that the mgr runs or
by an external tool that is activated by running the following command:

.. prompt:: bash $

   ceph device set-life-expectancy <device-id> <from> <to>

You can change the stored life expectancy manually, but such a change usually
doesn't accomplish anything. The reason for this is that whichever tool
originally set the stored life expectancy will probably undo your change by
setting it again, and a change to the stored value does not affect the actual
health of the hardware device.

DEVICE_HEALTH_IN_USE
____________________

One or more devices (that is, OSDs) are expected to fail soon and have been
marked ``out`` of the cluster (as controlled by
``mgr/devicehealth/mark_out_threshold``), but they are still participating in
one or more Placement Groups. This might be because the OSD(s) were marked
``out`` only recently and data is still migrating, or because data cannot be
migrated off of the OSD(s) for some reason (for example, the cluster is nearly
full, or the CRUSH hierarchy is structured so that there isn't another suitable
OSD to migrate the data to).

This message can be silenced by disabling self-heal behavior (that is, setting
``mgr/devicehealth/self_heal`` to ``false``), by adjusting
``mgr/devicehealth/mark_out_threshold``, or by addressing whichever condition
is preventing data from being migrated off of the ailing OSD(s).

.. _rados_health_checks_device_health_toomany:

DEVICE_HEALTH_TOOMANY
_____________________

Too many devices (that is, OSDs) are expected to fail soon, and because
``mgr/devicehealth/self_heal`` behavior is enabled, marking ``out`` all of the
ailing OSDs would exceed the cluster's ``mon_osd_min_in_ratio`` ratio.  This
ratio prevents a cascade of too many OSDs from being automatically marked
``out``.

You should promptly add new OSDs to the cluster to prevent data loss, or
incrementally replace the failing OSDs.

Alternatively, you can silence this health check by adjusting options including
``mon_osd_min_in_ratio`` or ``mgr/devicehealth/mark_out_threshold``.  Be
warned, however, that this will increase the likelihood of unrecoverable data
loss.


Data health (pools & placement groups)
--------------------------------------

PG_AVAILABILITY
_______________

Data availability is reduced. In other words, the cluster is unable to service
potential read or write requests for at least some data in the cluster.  More
precisely, one or more Placement Groups (PGs) are in a state that does not
allow I/O requests to be serviced. Any of the following PG states are
problematic if they do not clear quickly: *peering*, *stale*, *incomplete*, and
the lack of *active*.

For detailed information about which PGs are affected, run the following
command:

.. prompt:: bash $

   ceph health detail

In most cases, the root cause of this issue is that one or more OSDs are
currently ``down``: see ``OSD_DOWN`` above.

To see the state of a specific problematic PG, run the following command:

.. prompt:: bash $

   ceph tell <pgid> query

PG_DEGRADED
___________

Data redundancy is reduced for some data: in other words, the cluster does not
have the desired number of replicas for all data (in the case of replicated
pools) or erasure code fragments (in the case of erasure-coded pools).  More
precisely, one or more Placement Groups (PGs):

* have the *degraded* or *undersized* flag set, which means that there are not
  enough instances of that PG in the cluster; or
* have not had the *clean* state set for a long time.

For detailed information about which PGs are affected, run the following
command:

.. prompt:: bash $

   ceph health detail

In most cases, the root cause of this issue is that one or more OSDs are
currently "down": see ``OSD_DOWN`` above.

To see the state of a specific problematic PG, run the following command:

.. prompt:: bash $

   ceph tell <pgid> query


PG_RECOVERY_FULL
________________

Data redundancy might be reduced or even put at risk for some data due to a
lack of free space in the cluster. More precisely, one or more Placement Groups
have the *recovery_toofull* flag set, which means that the cluster is unable to
migrate or recover data because one or more OSDs are above the ``full``
threshold.

For steps to resolve this condition, see *OSD_FULL* above.

PG_BACKFILL_FULL
________________

Data redundancy might be reduced or even put at risk for some data due to a
lack of free space in the cluster. More precisely, one or more Placement Groups
have the *backfill_toofull* flag set, which means that the cluster is unable to
migrate or recover data because one or more OSDs are above the ``backfillfull``
threshold.

For steps to resolve this condition, see *OSD_BACKFILLFULL* above.

PG_DAMAGED
__________

Data scrubbing has discovered problems with data consistency in the cluster.
More precisely, one or more Placement Groups either (1) have the *inconsistent*
or ``snaptrim_error`` flag set, which indicates that an earlier data scrub
operation found a problem, or (2) have the *repair* flag set, which means that
a repair for such an inconsistency is currently in progress.

For more information, see :doc:`pg-repair`.

OSD_SCRUB_ERRORS
________________

Recent OSD scrubs have discovered inconsistencies. This alert is generally
paired with *PG_DAMAGED* (see above).

For more information, see :doc:`pg-repair`.

OSD_TOO_MANY_REPAIRS
____________________

The count of read repairs has exceeded the config value threshold
``mon_osd_warn_num_repaired`` (default: ``10``).  Because scrub handles errors
only for data at rest, and because any read error that occurs when another
replica is available will be repaired immediately so that the client can get
the object data, there might exist failing disks that are not registering any
scrub errors. This repair count is maintained as a way of identifying any such
failing disks.


LARGE_OMAP_OBJECTS
__________________

One or more pools contain large omap objects, as determined by
``osd_deep_scrub_large_omap_object_key_threshold`` (threshold for the number of
keys to determine what is considered a large omap object) or
``osd_deep_scrub_large_omap_object_value_sum_threshold`` (the threshold for the
summed size in bytes of all key values to determine what is considered a large
omap object) or both.  To find more information on object name, key count, and
size in bytes, search the cluster log for 'Large omap object found'. This issue
can be caused by RGW-bucket index objects that do not have automatic resharding
enabled. For more information on resharding, see :ref:`RGW Dynamic Bucket Index
Resharding <rgw_dynamic_bucket_index_resharding>`.

To adjust the thresholds mentioned above, run the following commands:

.. prompt:: bash $

   ceph config set osd osd_deep_scrub_large_omap_object_key_threshold <keys>
   ceph config set osd osd_deep_scrub_large_omap_object_value_sum_threshold <bytes>

CACHE_POOL_NEAR_FULL
____________________

A cache-tier pool is nearly full, as determined by the ``target_max_bytes`` and
``target_max_objects`` properties of the cache pool. Once the pool reaches the
target threshold, write requests to the pool might block while data is flushed
and evicted from the cache. This state normally leads to very high latencies
and poor performance.

To adjust the cache pool's target size, run the following commands:

.. prompt:: bash $

   ceph osd pool set <cache-pool-name> target_max_bytes <bytes>
   ceph osd pool set <cache-pool-name> target_max_objects <objects>

There might be other reasons that normal cache flush and evict activity are
throttled: for example, reduced availability of the base tier, reduced
performance of the base tier, or overall cluster load.

TOO_FEW_PGS
___________

The number of Placement Groups (PGs) that are in use in the cluster is below
the configurable threshold of ``mon_pg_warn_min_per_osd`` PGs per OSD. This can
lead to suboptimal distribution and suboptimal balance of data across the OSDs
in the cluster, and a reduction of overall performance.

If data pools have not yet been created, this condition is expected.

To address this issue, you can increase the PG count for existing pools or
create new pools.  For more information, see
:ref:`choosing-number-of-placement-groups`.

POOL_PG_NUM_NOT_POWER_OF_TWO
____________________________

One or more pools have a ``pg_num`` value that is not a power of two.  Although
this is not strictly incorrect, it does lead to a less balanced distribution of
data because some Placement Groups will have roughly twice as much data as
others have.

This is easily corrected by setting the ``pg_num`` value for the affected
pool(s) to a nearby power of two. To do so, run the following command:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_num <value>

To disable this health check, run the following command:

.. prompt:: bash $

   ceph config set global mon_warn_on_pool_pg_num_not_power_of_two false

POOL_TOO_FEW_PGS
________________

One or more pools should probably have more Placement Groups (PGs), given the
amount of data that is currently stored in the pool. This issue can lead to
suboptimal distribution and suboptimal balance of data across the OSDs in the
cluster, and a reduction of overall performance. This alert is raised only if
the ``pg_autoscale_mode`` property on the pool is set to ``warn``.

To disable the alert, entirely disable auto-scaling of PGs for the pool by
running the following command:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_autoscale_mode off

To allow the cluster to automatically adjust the number of PGs for the pool,
run the following command:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_autoscale_mode on

Alternatively, to manually set the number of PGs for the pool to the
recommended amount, run the following command:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_num <new-pg-num>

For more information, see :ref:`choosing-number-of-placement-groups` and
:ref:`pg-autoscaler`.

TOO_MANY_PGS
____________

The number of Placement Groups (PGs) in use in the cluster is above the
configurable threshold of ``mon_max_pg_per_osd`` PGs per OSD. If this threshold
is exceeded, the cluster will not allow new pools to be created, pool `pg_num`
to be increased, or pool replication to be increased (any of which, if allowed,
would lead to more PGs in the cluster). A large number of PGs can lead to
higher memory utilization for OSD daemons, slower peering after cluster state
changes (for example, OSD restarts, additions, or removals), and higher load on
the Manager and Monitor daemons.

The simplest way to mitigate the problem is to increase the number of OSDs in
the cluster by adding more hardware. Note that, because the OSD count that is
used for the purposes of this health check is the number of ``in`` OSDs,
marking ``out`` OSDs ``in`` (if there are any ``out`` OSDs available) can also
help. To do so, run the following command:

.. prompt:: bash $

   ceph osd in <osd id(s)>

For more information, see :ref:`choosing-number-of-placement-groups`.

POOL_TOO_MANY_PGS
_________________

One or more pools should probably have fewer Placement Groups (PGs), given the
amount of data that is currently stored in the pool. This issue can lead to
higher memory utilization for OSD daemons, slower peering after cluster state
changes (for example, OSD restarts, additions, or removals), and higher load on
the Manager and Monitor daemons. This alert is raised only if the
``pg_autoscale_mode`` property on the pool is set to ``warn``.

To disable the alert, entirely disable auto-scaling of PGs for the pool by
running the following command:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_autoscale_mode off

To allow the cluster to automatically adjust the number of PGs for the pool,
run the following command:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_autoscale_mode on

Alternatively, to manually set the number of PGs for the pool to the
recommended amount, run the following command:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_num <new-pg-num>

For more information, see :ref:`choosing-number-of-placement-groups` and
:ref:`pg-autoscaler`.


POOL_TARGET_SIZE_BYTES_OVERCOMMITTED
____________________________________

One or more pools have a ``target_size_bytes`` property that is set in order to
estimate the expected size of the pool, but the value(s) of this property are
greater than the total available storage (either by themselves or in
combination with other pools).

This alert is usually an indication that the ``target_size_bytes`` value for
the pool is too large and should be reduced or set to zero. To reduce the
``target_size_bytes`` value or set it to zero, run the following command:

.. prompt:: bash $

   ceph osd pool set <pool-name> target_size_bytes 0

The above command sets the value of ``target_size_bytes`` to zero. To set the
value of ``target_size_bytes`` to a non-zero value, replace the ``0`` with that
non-zero value.

For more information, see :ref:`specifying_pool_target_size`.

POOL_HAS_TARGET_SIZE_BYTES_AND_RATIO
____________________________________

One or more pools have both ``target_size_bytes`` and ``target_size_ratio`` set
in order to estimate the expected size of the pool.  Only one of these
properties should be non-zero. If both are set to a non-zero value, then
``target_size_ratio`` takes precedence and ``target_size_bytes`` is ignored.

To reset ``target_size_bytes`` to zero, run the following command:

.. prompt:: bash $

   ceph osd pool set <pool-name> target_size_bytes 0

For more information, see :ref:`specifying_pool_target_size`.

TOO_FEW_OSDS
____________

The number of OSDs in the cluster is below the configurable threshold of
``osd_pool_default_size``. This means that some or all data may not be able to
satisfy the data protection policy specified in CRUSH rules and pool settings.

SMALLER_PGP_NUM
_______________

One or more pools have a ``pgp_num`` value less than ``pg_num``. This alert is
normally an indication that the Placement Group (PG) count was increased
without any increase in the placement behavior.

This disparity is sometimes brought about deliberately, in order to separate
out the `split` step when the PG count is adjusted from the data migration that
is needed when ``pgp_num`` is changed.

This issue is normally resolved by setting ``pgp_num`` to match ``pg_num``, so
as to trigger the data migration, by running the following command:

.. prompt:: bash $

   ceph osd pool set <pool> pgp_num <pg-num-value>

MANY_OBJECTS_PER_PG
___________________

One or more pools have an average number of objects per Placement Group (PG)
that is significantly higher than the overall cluster average. The specific
threshold is determined by the ``mon_pg_warn_max_object_skew`` configuration
value.

This alert is usually an indication that the pool(s) that contain most of the
data in the cluster have too few PGs, or that other pools that contain less
data have too many PGs. See *TOO_MANY_PGS* above.

To silence the health check, raise the threshold by adjusting the
``mon_pg_warn_max_object_skew`` config option on the managers.

The health check will be silenced for a specific pool only if
``pg_autoscale_mode`` is set to ``on``.

POOL_APP_NOT_ENABLED
____________________

A pool exists that contains one or more objects, but the pool has not been
tagged for use by a particular application.

To resolve this issue, tag the pool for use by an application. For
example, if the pool is used by RBD, run the following command:

.. prompt:: bash $

   rbd pool init <poolname>

Alternatively, if the pool is being used by a custom application (here 'foo'),
you can label the pool by running the following low-level command:

.. prompt:: bash $

   ceph osd pool application enable foo

For more information, see :ref:`associate-pool-to-application`.

POOL_FULL
_________

One or more pools have reached (or are very close to reaching) their quota. The
threshold to raise this health check is determined by the
``mon_pool_quota_crit_threshold`` configuration option.

Pool quotas can be adjusted up or down (or removed) by running the following
commands:

.. prompt:: bash $

   ceph osd pool set-quota <pool> max_bytes <bytes>
   ceph osd pool set-quota <pool> max_objects <objects>

To disable a quota, set the quota value to 0.

POOL_NEAR_FULL
______________

One or more pools are approaching a configured fullness threshold.

One of the several thresholds that can raise this health check is determined by
the ``mon_pool_quota_warn_threshold`` configuration option.

Pool quotas can be adjusted up or down (or removed) by running the following
commands:

.. prompt:: bash $

   ceph osd pool set-quota <pool> max_bytes <bytes>
   ceph osd pool set-quota <pool> max_objects <objects>

To disable a quota, set the quota value to 0.

Other thresholds that can raise the two health checks above are
``mon_osd_nearfull_ratio`` and ``mon_osd_full_ratio``. For details and
resolution, see :ref:`storage-capacity` and :ref:`no-free-drive-space`.

OBJECT_MISPLACED
________________

One or more objects in the cluster are not stored on the node that CRUSH would
prefer that they be stored on. This alert is an indication that data migration
due to a recent cluster change has not yet completed.

Misplaced data is not a dangerous condition in and of itself; data consistency
is never at risk, and old copies of objects will not be removed until the
desired number of new copies (in the desired locations) has been created.

OBJECT_UNFOUND
______________

One or more objects in the cluster cannot be found. More precisely, the OSDs
know that a new or updated copy of an object should exist, but no such copy has
been found on OSDs that are currently online.

Read or write requests to unfound objects will block.

Ideally, a "down" OSD that has a more recent copy of the unfound object can be
brought back online. To identify candidate OSDs, check the peering state of the
PG(s) responsible for the unfound object. To see the peering state, run the
following command:

.. prompt:: bash $

   ceph tell <pgid> query

On the other hand, if the latest copy of the object is not available, the
cluster can be told to roll back to a previous version of the object. For more
information, see :ref:`failures-osd-unfound`.

SLOW_OPS
________

One or more OSD requests or monitor requests are taking a long time to process.
This alert might be an indication of extreme load, a slow storage device, or a
software bug.

To query the request queue for the daemon that is causing the slowdown, run the
following command from the daemon's host:

.. prompt:: bash $

   ceph daemon osd.<id> ops

To see a summary of the slowest recent requests, run the following command:

.. prompt:: bash $

   ceph daemon osd.<id> dump_historic_ops

To see the location of a specific OSD, run the following command:

.. prompt:: bash $

   ceph osd find osd.<id>

PG_NOT_SCRUBBED
_______________

One or more PGs has not been scrubbed recently.  PGs are normally scrubbed
within every configured interval specified by
:ref:`osd_scrub_max_interval <osd_scrub_max_interval>` globally. This
interval can be overriden on per-pool basis with
:ref:`scrub_max_interval <scrub_max_interval>`. The warning triggers when
``mon_warn_pg_not_scrubbed_ratio`` percentage of interval has elapsed without a
scrub since it was due.

PGs will be scrubbed only if they are flagged as ``clean`` (which means that
they are to be cleaned, and not that they have been examined and found to be
clean). Misplaced or degraded PGs will not be flagged as ``clean`` (see
*PG_AVAILABILITY* and *PG_DEGRADED* above).

To manually initiate a scrub of a clean PG, run the following command:

.. prompt: bash $

   ceph pg scrub <pgid>

PG_NOT_DEEP_SCRUBBED
____________________

One or more PGs has not been deep scrubbed recently.  PGs are normally
scrubbed every ``osd_deep_scrub_interval`` seconds, and this warning
triggers when ``mon_warn_pg_not_deep_scrubbed_ratio`` percentage of interval has elapsed
without a scrub since it was due.

PGs will receive a deep scrub only if they are flagged as *clean* (which means
that they are to be cleaned, and not that they have been examined and found to
be clean). Misplaced or degraded PGs might not be flagged as ``clean`` (see
*PG_AVAILABILITY* and *PG_DEGRADED* above).

To manually initiate a deep scrub of a clean PG, run the following command:

.. prompt:: bash $

   ceph pg deep-scrub <pgid>


PG_SLOW_SNAP_TRIMMING
_____________________

The snapshot trim queue for one or more PGs has exceeded the configured warning
threshold. This alert indicates either that an extremely large number of
snapshots was recently deleted, or that OSDs are unable to trim snapshots
quickly enough to keep up with the rate of new snapshot deletions.

The warning threshold is determined by the ``mon_osd_snap_trim_queue_warn_on``
option (default: 32768).

This alert might be raised if OSDs are under excessive load and unable to keep
up with their background work, or if the OSDs' internal metadata database is
heavily fragmented and unable to perform. The alert might also indicate some
other performance issue with the OSDs.

The exact size of the snapshot trim queue is reported by the ``snaptrimq_len``
field of ``ceph pg ls -f json-detail``.

Stretch Mode
------------

INCORRECT_NUM_BUCKETS_STRETCH_MODE
__________________________________

Stretch mode currently only support 2 dividing buckets with OSDs, this warning suggests
that the number of dividing buckets is not equal to 2 after stretch mode is enabled.
You can expect unpredictable failures and MON assertions until the condition is fixed.

We encourage you to fix this by removing additional dividing buckets or bump the
number of dividing buckets to 2.

UNEVEN_WEIGHTS_STRETCH_MODE
___________________________

The 2 dividing buckets must have equal weights when stretch mode is enabled.
This warning suggests that the 2 dividing buckets have uneven weights after
stretch mode is enabled. This is not immediately fatal, however, you can expect
Ceph to be confused when trying to process transitions between dividing buckets.

We encourage you to fix this by making the weights even on both dividing buckets.
This can be done by making sure the combined weight of the OSDs on each dividing
bucket are the same.

Miscellaneous
-------------

RECENT_CRASH
____________

One or more Ceph daemons have crashed recently, and the crash(es) have not yet
been acknowledged and archived by the administrator. This alert might indicate
a software bug, a hardware problem (for example, a failing disk), or some other
problem.

To list recent crashes, run the following command:

.. prompt:: bash $

   ceph crash ls-new

To examine information about a specific crash, run the following command:

.. prompt:: bash $

   ceph crash info <crash-id>

To silence this alert, you can archive the crash (perhaps after the crash
has been examined by an administrator) by running the following command:

.. prompt:: bash $

   ceph crash archive <crash-id>

Similarly, to archive all recent crashes, run the following command:

.. prompt:: bash $

   ceph crash archive-all

Archived crashes will still be visible by running the command ``ceph crash
ls``, but not by running the command ``ceph crash ls-new``.

The time period that is considered recent is determined by the option
``mgr/crash/warn_recent_interval`` (default: two weeks).

To entirely disable this alert, run the following command:

.. prompt:: bash $

   ceph config set mgr/crash/warn_recent_interval 0

RECENT_MGR_MODULE_CRASH
_______________________

One or more ``ceph-mgr`` modules have crashed recently, and the crash(es) have
not yet been acknowledged and archived by the administrator.  This alert
usually indicates a software bug in one of the software modules that are
running inside the ``ceph-mgr`` daemon. The module that experienced the problem
might be disabled as a result, but other modules are unaffected and continue to
function as expected.

As with the *RECENT_CRASH* health check, a specific crash can be inspected by
running the following command:

.. prompt:: bash $

   ceph crash info <crash-id>

To silence this alert, you can archive the crash (perhaps after the crash has
been examined by an administrator) by running the following command:

.. prompt:: bash $

   ceph crash archive <crash-id>

Similarly, to archive all recent crashes, run the following command:

.. prompt:: bash $

   ceph crash archive-all

Archived crashes will still be visible by running the command ``ceph crash ls``
but not by running the command ``ceph crash ls-new``.

The time period that is considered recent is determined by the option
``mgr/crash/warn_recent_interval`` (default: two weeks).

To entirely disable this alert, run the following command:

.. prompt:: bash $

   ceph config set mgr/crash/warn_recent_interval 0

TELEMETRY_CHANGED
_________________

Telemetry has been enabled, but because the contents of the telemetry report
have changed in the meantime, telemetry reports will not be sent.

Ceph developers occasionally revise the telemetry feature to include new and
useful information, or to remove information found to be useless or sensitive.
If any new information is included in the report, Ceph requires the
administrator to re-enable telemetry. This requirement ensures that the
administrator has an opportunity to (re)review the information that will be
shared.

To review the contents of the telemetry report, run the following command:

.. prompt:: bash $

   ceph telemetry show

Note that the telemetry report consists of several channels that may be
independently enabled or disabled. For more information, see :ref:`telemetry`.

To re-enable telemetry (and silence the alert), run the following command:

.. prompt:: bash $

   ceph telemetry on

To disable telemetry (and silence the alert), run the following command:

.. prompt:: bash $

   ceph telemetry off

AUTH_BAD_CAPS
_____________

One or more auth users have capabilities that cannot be parsed by the monitors.
As a general rule, this alert indicates that there are one or more daemon types
that the user is not authorized to use to perform any action.

This alert is most likely to be raised after an upgrade if (1) the capabilities
were set with an older version of Ceph that did not properly validate the
syntax of those capabilities, or if (2) the syntax of the capabilities has
changed.

To remove the user(s) in question, run the following command:

.. prompt:: bash $

   ceph auth rm <entity-name>

(This resolves the health check, but it prevents clients from being able to
authenticate as the removed user.)

Alternatively, to update the capabilities for the user(s), run the following
command:

.. prompt:: bash $

   ceph auth <entity-name> <daemon-type> <caps> [<daemon-type> <caps> ...]

For more information about auth capabilities, see :ref:`user-management`.

OSD_NO_DOWN_OUT_INTERVAL
________________________

The ``mon_osd_down_out_interval`` option is set to zero, which means that the
system does not automatically perform any repair or healing operations when an
OSD fails. Instead, an administrator an external orchestrator must manually
mark "down" OSDs as ``out`` (by running ``ceph osd out <osd-id>``) in order to
trigger recovery.

This option is normally set to five or ten minutes, which should be enough time
for a host to power-cycle or reboot.

To silence this alert, set ``mon_warn_on_osd_down_out_interval_zero`` to
``false`` by running the following command:

.. prompt:: bash $

   ceph config global mon mon_warn_on_osd_down_out_interval_zero false

DASHBOARD_DEBUG
_______________

The Dashboard debug mode is enabled. This means that if there is an error while
processing a REST API request, the HTTP error response will contain a Python
traceback. This mode should be disabled in production environments because such
a traceback might contain and expose sensitive information.

To disable the debug mode, run the following command:

.. prompt:: bash $

   ceph dashboard debug disable
