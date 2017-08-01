
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

One or more OSDs are marked down

OSD_<crush type>_DOWN
_____________________

(e.g. OSD_HOST_DOWN, OSD_ROOT_DOWN)

All the OSDs within a particular CRUSH subtree are marked down, for example
all OSDs on a host.

OSD_ORPHAN
__________


OSD_OUT_OF_ORDER_FULL
_____________________


OSD_FULL
________


OSD_BACKFILLFULL
________________


OSD_NEARFULL
____________


OSDMAP_FLAGS
____________


OSD_FLAGS
_________


OLD_CRUSH_TUNABLES
__________________


OLD_CRUSH_STRAW_CALC_VERSION
____________________________


CACHE_POOL_NO_HIT_SET
_____________________


OSD_NO_SORTBITWISE
__________________


POOL_FULL
_________


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


POOL_APP_NOT_ENABLED
____________________

A pool exists that contains one or more objects but has not been
tagged for use by a particular application.

Resolve this warning by labeling the pool for use by an application.  For
example, if the pool is used by RBD,::

  rbd pool init <poolname>

If the pool is being used by a custom application 'foo', you can also label
via the low-level command::

  ceph osd pool application enable foo

For more information, see :doc:`pools.rst#associate-pool-to-application`.

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


