=====================
 Troubleshooting PGs
=====================

Placement Groups Never Get Clean
================================

When you create a cluster and your cluster remains in ``active``, 
``active+remapped`` or ``active+degraded`` status and never achieve an 
``active+clean`` status, you likely have a problem with your configuration.

You may need to review settings in the `Pool, PG and CRUSH Config Reference`_
and make appropriate adjustments.

As a general rule, you should run your cluster with more than one OSD and a
pool size greater than 1 object replica.

One Node Cluster
----------------

Ceph no longer provides documentation for operating on a single node, because
you would never deploy a system designed for distributed computing on a single
node. Additionally, mounting client kernel modules on a single node containing a
Ceph  daemon may cause a deadlock due to issues with the Linux kernel itself
(unless you use VMs for the clients). You can experiment with Ceph in a 1-node
configuration, in spite of the limitations as described herein.

If you are trying to create a cluster on a single node, you must change the
default of the ``osd crush chooseleaf type`` setting from ``1`` (meaning 
``host`` or ``node``) to ``0`` (meaning ``osd``) in your Ceph configuration
file before you create your monitors and OSDs. This tells Ceph that an OSD
can peer with another OSD on the same host. If you are trying to set up a
1-node cluster and ``osd crush chooseleaf type`` is greater than ``0``, 
Ceph will try to peer the PGs of one OSD with the PGs of another OSD on 
another node, chassis, rack, row, or even datacenter depending on the setting.

.. tip:: DO NOT mount kernel clients directly on the same node as your 
   Ceph Storage Cluster, because kernel conflicts can arise. However, you 
   can mount kernel clients within virtual machines (VMs) on a single node.

If you are creating OSDs using a single disk, you must create directories
for the data manually first. For example:: 

	mkdir /var/local/osd0 /var/local/osd1
	ceph-deploy osd prepare {localhost-name}:/var/local/osd0 {localhost-name}:/var/local/osd1
	ceph-deploy osd activate {localhost-name}:/var/local/osd0 {localhost-name}:/var/local/osd1


Fewer OSDs than Replicas
------------------------

If you've brought up two OSDs to an ``up`` and ``in`` state, but you still 
don't see ``active + clean`` placement groups, you may have an 
``osd pool default size`` set to greater than ``2``.

There are a few ways to address this situation. If you want to operate your
cluster in an ``active + degraded`` state with two replicas, you can set the 
``osd pool default min size`` to ``2`` so that you can write objects in 
an ``active + degraded`` state. You may also set the ``osd pool default size``
setting to ``2`` so that you only have two stored replicas (the original and 
one replica), in which case the cluster should achieve an ``active + clean`` 
state.

.. note:: You can make the changes at runtime. If you make the changes in 
   your Ceph configuration file, you may need to restart your cluster.


Pool Size = 1
-------------

If you have the ``osd pool default size`` set to ``1``, you will only have 
one copy of the object. OSDs rely on other OSDs to tell them which objects 
they should have. If a first OSD has a copy of an object and there is no
second copy, then no second OSD can tell the first OSD that it should have
that copy. For each placement group mapped to the first OSD (see 
``ceph pg dump``), you can force the first OSD to notice the placement groups
it needs by running::
   
   	ceph pg force_create_pg <pgid>
   	

CRUSH Map Errors
----------------

Another candidate for placement groups remaining unclean involves errors 
in your CRUSH map.


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
 * OSD is down
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


Only a Few OSDs Receive Data
============================

If you have many nodes in your cluster and only a few of them receive data,
`check`_ the number of placement groups in your pool. Since placement groups get
mapped to OSDs, a small number of placement groups will not distribute across
your cluster. Try creating a pool with a placement group count that is a
multiple of the number of OSDs. See `Placement Groups`_ for details. The default
placement group count for pools isn't useful, but you can change it `here`_.


Can't Write Data
================

If your cluster is up, but some OSDs are down and you cannot write data, 
check to ensure that you have the minimum number of OSDs running for the
placement group. If you don't have the minimum number of OSDs running, 
Ceph will not allow you to write data because there is no guarantee
that Ceph can replicate your data. See ``osd pool default min size``
in the `Pool, PG and CRUSH Config Reference`_ for details.


PGs Inconsistent
================

If you receive an ``active + clean + inconsistent`` state, this may happen
due to an error during scrubbing. If the inconsistency is due to disk errors,
check your disks.

You can repair the inconsistent placement group by executing:: 

	ceph pg repair {placement-group-ID}

If you receive ``active + clean + inconsistent`` states periodically due to 
clock skew, you may consider configuring your `NTP`_ daemons on your 
monitor hosts to act as peers. See `The Network Time Protocol`_ and Ceph 
`Clock Settings`_ for additional details.



.. _check: ../../operations/placement-groups#get-the-number-of-placement-groups
.. _here: ../../configuration/pool-pg-config-ref
.. _Placement Groups: ../../operations/placement-groups
.. _Pool, PG and CRUSH Config Reference: ../../configuration/pool-pg-config-ref
.. _NTP: http://en.wikipedia.org/wiki/Network_Time_Protocol
.. _The Network Time Protocol: http://www.ntp.org/
.. _Clock Settings: ../../configuration/mon-config-ref/#clock


