==============================
 Recovering from OSD Failures
==============================

Single OSD Failure
==================

When a ``ceph-osd`` process dies, the monitor will learn about the failure
from surviving ``ceph-osd`` daemons and report it via the ``ceph health``
command::

	ceph health
	HEALTH_WARN 1/3 in osds are down

Specifically, you will get a warning whenever there are ``ceph-osd``
processes that are marked ``in`` and ``down``.  You can identify which
``ceph-osds`` are ``down`` with::

	ceph health detail
	HEALTH_WARN 1/3 in osds are down
	osd.0 is down since epoch 23, last address 192.168.106.220:6800/11080

Under normal circumstances, simply restarting the ``ceph-osd`` daemon will
allow it to rejoin the cluster and recover.  If there is a disk
failure or other fault preventing ``ceph-osd`` from functioning or
restarting, an error message should be present in its log file in
``/var/log/ceph``.  

If the daemon stopped because of a heartbeat failure, the underlying
kernel file system may be unresponsive. Check ``dmesg`` output for disk
or other kernel errors.

If the problem is a software error (failed assertion or other
unexpected error), it should be reported to the :ref:`mailing list
<mailing-list>`.


The Cluster Has No Free Disk Space
==================================

If the cluster fills up, the monitor will prevent new data from being
written.  The system puts ``ceph-osds`` in two categories: ``nearfull``
and ``full``, with configurable threshholds for each (80% and 90% by
default).  In both cases, full ``ceph-osds`` will be reported by ``ceph health``::

	ceph health
	HEALTH_WARN 1 nearfull osds
	osd.2 is near full at 85%

Or::

	ceph health
	HEALTH_ERR 1 nearfull osds, 1 full osds
	osd.2 is near full at 85%
	osd.3 is full at 97%

The best way to deal with a full cluster is to add new ``ceph-osds``,
allowing the cluster to redistribute data to the newly available
storage.


Homeless Placement Groups
=========================

It is possible for all OSDs that had copies of a given placement groups to fail.
If that's the case, that subset of the object store is unavailable, and the
monitor will receive no status updates for those placement groups.  To detect
this situation, the monitor marks any placement group whose primary OSD has
failed as ``stale``.  For example::

	ceph health
	HEALTH_WARN 24 pgs stale; 3/300 in osds are down

You can identify which placement groups are ``stale``, and what the last OSDs to
store them were, with::

	ceph health detail
	HEALTH_WARN 24 pgs stale; 3/300 in osds are down
	...
	pg 2.5 is stuck stale+active+remapped, last acting [2,0]
	...
	osd.10 is down since epoch 23, last address 192.168.106.220:6800/11080
	osd.11 is down since epoch 13, last address 192.168.106.220:6803/11539
	osd.12 is down since epoch 24, last address 192.168.106.220:6806/11861

If we want to get placement group 2.5 back online, for example, this tells us that
it was last managed by ``osd.0`` and ``osd.2``.  Restarting those ``ceph-osd``
daemons will allow the cluster to recover that placement group (and, presumably,
many others).


Stuck Placement Groups
======================

It is normal for placement groups to enter states like "degraded" or "peering"
following a failure.  Normally these states indicate the normal progression
through the failure recovery process. However, if a placement group stays in one
of these states for a long time this may be an indication of a larger problem.
For this reason, the monitor will warn when placement groups get "stuck" in a
non-optimal state.  Specifically, we check for:

* ``inactive`` - The placement group has not been ``active`` for too long 
  (i.e., it hasn't been able to service read/write requests).
  
* ``unclean`` - The placement group has not been ``clean`` for too long 
  (i.e., it hasn't been able to completely recover from a previous failure).

* ``stale`` - The placement group status has not been updated by a ``ceph-osd``,
  indicating that all nodes storing this placement group may be ``down``.

You can explicitly list stuck placement groups with one of::

	ceph pg dump_stuck stale
	ceph pg dump_stuck inactive
	ceph pg dump_stuck unclean

For stuck ``stale`` placement groups, it is normally a matter of getting the
right ``ceph-osd`` daemons running again.  For stuck ``inactive`` placement
groups, it is usually a peering problem (see :ref:`failures-osd-peering`).  For
stuck ``unclean`` placement groups, there is usually something preventing
recovery from completing, like unfound objects (see
:ref:`failures-osd-unfound`);


.. _failures-osd-peering:

Placement Group Down - Peering Failure
======================================

In certain cases, the ``ceph-osd`` `Peering` process can run into
problems, preventing a PG from becoming active and usable.  For
example, ``ceph health`` might report::

	ceph health detail
	HEALTH_ERR 7 pgs degraded; 12 pgs down; 12 pgs peering; 1 pgs recovering; 6 pgs stuck unclean; 114/3300 degraded (3.455%); 1/3 in osds are down
	...
	pg 0.5 is down+peering
	pg 1.4 is down+peering
	...
	osd.1 is down since epoch 69, last address 192.168.106.220:6801/8651

We can query the cluster to determine exactly why the PG is marked ``down`` with::

	ceph pg 0.5 query

.. code-block:: javascript

 { "state": "down+peering",
   ...
   "recovery_state": [
        { "name": "Started\/Primary\/Peering\/GetInfo",
          "enter_time": "2012-03-06 14:40:16.169679",
          "requested_info_from": []},
        { "name": "Started\/Primary\/Peering",
          "enter_time": "2012-03-06 14:40:16.169659",
          "probing_osds": [
                0,
                1],
          "blocked": "peering is blocked due to down osds",
          "down_osds_we_would_probe": [
                1],
          "peering_blocked_by": [
                { "osd": 1,
                  "current_lost_at": 0,
                  "comment": "starting or marking this osd lost may let us proceed"}]},
        { "name": "Started",
          "enter_time": "2012-03-06 14:40:16.169513"}
    ]
 }

The ``recovery_state`` section tells us that peering is blocked due to
down ``ceph-osd`` daemons, specifically ``osd.1``.  In this case, we can start that ``ceph-osd``
and things will recover.

Alternatively, if there is a catastrophic failure of ``osd.1`` (e.g., disk
failure), we can tell the cluster that it is ``lost`` and to cope as
best it can. 

.. important:: This is dangerous in that the cluster cannot
   guarantee that the other copies of the data are consistent 
   and up to date.  

To instruct Ceph to continue anyway::

	ceph osd lost 1

Recovery will proceed.


.. _failures-osd-unfound:

Unfound Objects
===============

Under certain combinations of failures Ceph may complain about
``unfound`` objects::

	ceph health detail
	HEALTH_WARN 1 pgs degraded; 78/3778 unfound (2.065%)
	pg 2.4 is active+degraded, 78 unfound

This means that the storage cluster knows that some objects (or newer
copies of existing objects) exist, but it hasn't found copies of them.
One example of how this might come about for a PG whose data is on ceph-osds
1 and 2:

* 1 goes down
* 2 handles some writes, alone
* 1 comes up
* 1 and 2 repeer, and the objects missing on 1 are queued for recovery.
* Before the new objects are copied, 2 goes down.

Now 1 knows that these object exist, but there is no live ``ceph-osd`` who
has a copy.  In this case, IO to those objects will block, and the
cluster will hope that the failed node comes back soon; this is
assumed to be preferable to returning an IO error to the user.

First, you can identify which objects are unfound with::

	ceph pg 2.4 list_missing [starting offset, in json]

.. code-block:: javascript

 { "offset": { "oid": "",
      "key": "",
      "snapid": 0,
      "hash": 0,
      "max": 0},
  "num_missing": 0,
  "num_unfound": 0,
  "objects": [
     { "oid": "object 1",
       "key": "",
       "hash": 0,
       "max": 0 },
     ...
  ],
  "more": 0}

If there are too many objects to list in a single result, the ``more``
field will be true and you can query for more.  (Eventually the
command line tool will hide this from you, but not yet.)

Second, you can identify which OSDs have been probed or might contain
data::

	ceph pg 2.4 query

.. code-block:: javascript

   "recovery_state": [
        { "name": "Started\/Primary\/Active",
          "enter_time": "2012-03-06 15:15:46.713212",
          "might_have_unfound": [
                { "osd": 1,
                  "status": "osd is down"}]},

In this case, for example, the cluster knows that ``osd.1`` might have
data, but it is ``down``.  The full range of possible states include::

 * already probed
 * querying
 * osd is down
 * not queried (yet)

Sometimes it simply takes some time for the cluster to query possible
locations.  

It is possible that there are other locations where the object can
exist that are not listed.  For example, if a ceph-osd is stopped and
taken out of the cluster, the cluster fully recovers, and due to some
future set of failures ends up with an unfound object, it won't
consider the long-departed ceph-osd as a potential location to
consider.  (This scenario, however, is unlikely.)

If all possible locations have been queried and objects are still
lost, you may have to give up on the lost objects. This, again, is
possible given unusual combinations of failures that allow the cluster
to learn about writes that were performed before the writes themselves
are recovered.  To mark the "unfound" objects as "lost"::

	ceph pg 2.5 mark_unfound_lost revert

This the final argument specifies how the cluster should deal with
lost objects.  Currently the only supported option is "revert", which
will either roll back to a previous version of the object or (if it
was a new object) forget about it entirely.  Use this with caution, as
it may confuse applications that expected the object to exist.



Slow or Unresponsive OSD
========================

If, for some reason, a ``ceph-osd`` is slow to respond to a request, it will
generate log messages complaining about requests that are taking too
long.  The warning threshold defaults to 30 seconds, and is configurable
via the ``osd op complaint time`` option.  When this happens, the cluster
log will receive messages like::

    slow request 30.383883 seconds old, received at 2013-02-12 16:27:15.508374: osd_op(client.9821.0:122242 rb.0.209f.74b0dc51.000000000120 [write 921600~4096] 2.981cf6bc) v4 currently no flag points reached

Possible causes include:

 * bad disk (check ``dmesg`` output)
 * kernel file system bug (check ``dmesg`` output)
 * overloaded cluster (check system load, iostat, etc.)
 * ceph-osd bug

Pay particular attention to the ``currently`` part, as that will give
some clue as to what the request is waiting for.  You can further look
at exactly what requests the slow OSD is working on are, and what
state(s) they are in with::

 ceph --admin-daemon /var/run/ceph/ceph-osd.{ID}.asok dump_ops_in_flight

These are sorted oldest to newest, and the dump includes an ``age``
indicating how long the request has been in the queue.


Flapping OSDs
=============

If something is causing OSDs to "flap" (repeatedly getting marked ``down`` and then
``up`` again), you can force the monitors to stop with::

	ceph osd set noup      # prevent osds from getting marked up
	ceph osd set nodown    # prevent osds from getting marked down

These flags are recorded in the osdmap structure::

	ceph osd dump | grep flags
	flags no-up,no-down

You can clear the flags with::

	ceph osd unset noup
	ceph osd unset nodown

Two other flags are supported, ``noin`` and ``noout``, which prevent
booting OSDs from being marked ``in`` (allocated data) or down
ceph-osds from eventually being marked ``out`` (regardless of what the
current value for ``mon osd down out interval`` is).

Note that ``noup``, ``noout``, and ``noout`` are temporary in the
sense that once the flags are cleared, the action they were blocking
should occur shortly after.  The ``noin`` flag, on the other hand,
prevents ceph-osds from being marked in on boot, and any daemons that
started while the flag was set will remain that way.
