
=============
Health checks
=============

Overview
========

There is a finite set of possible health messages that a Ceph cluster can
raise -- these are defined as *health checks* which have unique identifiers.

The identifier is a terse pseudo-human-readable (i.e. like a variable name)
string.  It is intended to enable tools (such as UIs) to make sense of
health checks, and present them in a way that reflects their meaning.

This page lists the health checks that are raised by the monitor and manager
daemons.  In addition to these, you may also see health checks that originate
from MDS daemons (see :doc:`/cephfs/health-messages`), and health checks
that are defined by ceph-mgr python modules.

Definitions
===========


OSDs
----

OSD_DOWN
________

One or more OSDs are marked down.  The ceph-osd daemon may have been
stopped, or peer OSDs may be unable to reach the OSD over the network.
Common causes include a stopped or crashed daemon, a down host, or a
network outage.

Verify the host is healthy, the daemon is started, and network is
functioning.  If the daemon has crashed, the daemon log file
(``/var/log/ceph/ceph-osd.*``) may contain debugging information.

OSD_<crush type>_DOWN 
_____________________

(e.g. OSD_HOST_DOWN, OSD_ROOT_DOWN)

All the OSDs within a particular CRUSH subtree are marked down, for example
all OSDs on a host.

OSD_ORPHAN
__________

An OSD is referenced in the CRUSH map hierarchy but does not exist.

The OSD can be removed from the CRUSH hierarchy with::

  ceph osd crush rm osd.<id>

OSD_OUT_OF_ORDER_FULL
_____________________

The utilization thresholds for `backfillfull`, `nearfull`, `full`,
and/or `failsafe_full` are not ascending.  In particular, we expect
`backfillfull < nearfull`, `nearfull < full`, and `full <
failsafe_full`.

The thresholds can be adjusted with::

  ceph osd set-backfillfull-ratio <ratio>
  ceph osd set-nearfull-ratio <ratio>
  ceph osd set-full-ratio <ratio>


OSD_FULL
________

One or more OSDs has exceeded the `full` threshold and is preventing
the cluster from servicing writes.

Utilization by pool can be checked with::

  ceph df

The currently defined `full` ratio can be seen with::

  ceph osd dump | grep full_ratio

A short-term workaround to restore write availability is to raise the full
threshold by a small amount::

  ceph osd set-full-ratio <ratio>

New storage should be added to the cluster by deploying more OSDs or
existing data should be deleted in order to free up space.
  
OSD_BACKFILLFULL
________________

One or more OSDs has exceeded the `backfillfull` threshold, which will
prevent data from being allowed to rebalance to this device.  This is
an early warning that rebalancing may not be able to complete and that
the cluster is approaching full.

Utilization by pool can be checked with::

  ceph df

OSD_NEARFULL
____________

One or more OSDs has exceeded the `nearfull` threshold.  This is an early
warning that the cluster is approaching full.

Utilization by pool can be checked with::

  ceph df

OSDMAP_FLAGS
____________

One or more cluster flags of interest has been set.  These flags include:

* *full* - the cluster is flagged as full and cannot service writes
* *pauserd*, *pausewr* - paused reads or writes
* *noup* - OSDs are not allowed to start
* *nodown* - OSD failure reports are being ignored, such that the
  monitors will not mark OSDs `down`
* *noin* - OSDs that were previously marked `out` will not be marked
  back `in` when they start
* *noout* - down OSDs will not automatically be marked out after the
  configured interval
* *nobackfill*, *norecover*, *norebalance* - recovery or data
  rebalancing is suspended
* *noscrub*, *nodeep_scrub* - scrubbing is disabled
* *notieragent* - cache tiering activity is suspended

With the exception of *full*, these flags can be set or cleared with::

  ceph osd set <flag>
  ceph osd unset <flag>
    
OSD_FLAGS
_________

One or more OSDs has a per-OSD flag of interest set.  These flags include:

* *noup*: OSD is not allowed to start
* *nodown*: failure reports for this OSD will be ignored
* *noin*: if this OSD was previously marked `out` automatically
  after a failure, it will not be marked in when it stats
* *noout*: if this OSD is down it will not automatically be marked
  `out` after the configured interval

Per-OSD flags can be set and cleared with::

  ceph osd add-<flag> <osd-id>
  ceph osd rm-<flag> <osd-id>

For example, ::

  ceph osd rm-nodown osd.123

OLD_CRUSH_TUNABLES
__________________

The CRUSH map is using very old settings and should be updated.  The
oldest tunables that can be used (i.e., the oldest client version that
can connect to the cluster) without triggering this health warning is
determined by the ``mon_crush_min_required_version`` config option.
See :doc:`/rados/operations/crush-map/#tunables` for more information.

OLD_CRUSH_STRAW_CALC_VERSION
____________________________

The CRUSH map is using an older, non-optimal method for calculating
intermediate weight values for ``straw`` buckets.

The CRUSH map should be updated to use the newer method
(``straw_calc_version=1``).  See
:doc:`/rados/operations/crush-map/#tunables` for more information.

CACHE_POOL_NO_HIT_SET
_____________________

One or more cache pools is not configured with a *hit set* to track
utilization, which will prevent the tiering agent from identifying
cold objects to flush and evict from the cache.

Hit sets can be configured on the cache pool with::

  ceph osd pool set <poolname> hit_set_type <type>
  ceph osd pool set <poolname> hit_set_period <period-in-seconds>
  ceph osd pool set <poolname> hit_set_count <number-of-hitsets>
  ceph osd pool set <poolname> hit_set_fpp <target-false-positive-rate>  

OSD_NO_SORTBITWISE
__________________

No pre-luminous v12.y.z OSDs are running but the ``sortbitwise`` flag has not
been set.

The ``sortbitwise`` flag must be set before luminous v12.y.z or newer
OSDs can start.  You can safely set the flag with::

  ceph osd set sortbitwise

POOL_FULL
_________

One or more pools has reached its quota and is no longer allowing writes.

Pool quotas and utilization can be seen with::

  ceph df detail

You can either raise the pool quota with::

  ceph osd pool set-quota <poolname> max_objects <num-objects>
  ceph osd pool set-quota <poolname> max_bytes <num-bytes>

or delete some existing data to reduce utilization.

Data health (pools & placement groups)
------------------------------

PG_AVAILABILITY
_______________


PG_DEGRADED
___________


PG_DEGRADED_FULL
________________


PG_DAMAGED
__________

OSD_SCRUB_ERRORS
________________


CACHE_POOL_NEAR_FULL
____________________


TOO_FEW_PGS
___________


TOO_MANY_PGS
____________


SMALLER_PGP_NUM
_______________


MANY_OBJECTS_PER_PG
___________________


POOL_FULL
_________


POOL_NEAR_FULL
______________


OBJECT_MISPLACED
________________


OBJECT_UNFOUND
______________


REQUEST_SLOW
____________


REQUEST_STUCK
_____________


PG_NOT_SCRUBBED
_______________


PG_NOT_DEEP_SCRUBBED
____________________


CephFS
------

FS_WITH_FAILED_MDS
__________________


FS_DEGRADED
___________


MDS_INSUFFICIENT_STANDBY
________________________


MDS_DAMAGED
___________


