=========================
 Monitoring OSDs and PGs
=========================

High availability and high reliability require a fault-tolerant approach to
managing hardware and software issues. Ceph has no single point of failure and
it can service requests for data even when in a "degraded" mode. Ceph's `data
placement`_ introduces a layer of indirection to ensure that data doesn't bind
directly to specific OSDs. For this reason, tracking system faults
requires finding the `placement group`_ (PG) and the underlying OSDs at the
root of the problem.

.. tip:: A fault in one part of the cluster might prevent you from accessing a 
   particular object, but that doesn't mean that you are prevented from accessing other objects.
   When you run into a fault, don't panic. Just follow the steps for monitoring
   your OSDs and placement groups, and then begin troubleshooting.

Ceph is generally self-repairing. However, when problems persist and you want to find out 
what exactly is going wrong, it can be helpful to monitor OSDs and PGs.


Monitoring OSDs
===============

An OSD's status is as follows: it is either in the cluster (``in``) or out of the cluster
(``out``); likewise, it is either up and running (``up``) or down and not
running (``down``). If an OSD is ``up``, it can be either ``in`` the cluster
(if so, you can read and write data) or ``out`` of the cluster. If the OSD was previously
``in`` the cluster but was recently moved ``out`` of the cluster, Ceph will migrate its
PGs to other OSDs. If an OSD is ``out`` of the cluster, CRUSH will                               
not assign any PGs to that OSD. If an OSD is ``down``, it should also be
``out``.

.. note:: If an OSD is ``down`` and ``in``, then there is a problem and the cluster 
   is not in a healthy state.

.. ditaa::

           +----------------+        +----------------+
           |                |        |                |
           |   OSD #n In    |        |   OSD #n Up    |
           |                |        |                |
           +----------------+        +----------------+
                   ^                         ^
                   |                         |
                   |                         |
                   v                         v
           +----------------+        +----------------+
           |                |        |                |
           |   OSD #n Out   |        |   OSD #n Down  |
           |                |        |                |
           +----------------+        +----------------+

If you run the commands ``ceph health``, ``ceph -s``, or ``ceph -w``,
you might notice that the cluster does not always show ``HEALTH OK``. Don't
panic. There are certain circumstances in which it is expected and normal that
the cluster will **NOT** show ``HEALTH OK``:

#. You haven't started the cluster yet.
#. You have just started or restarted the cluster and it's not ready to show
   health statuses yet, because the PGs are in the process of being created and
   the OSDs are in the process of peering.
#. You have just added or removed an OSD.
#. You have just have modified your cluster map.

Checking to see if OSDs are ``up`` and running is an important aspect of monitoring them:
whenever the cluster is up and running, every OSD that is ``in`` the cluster should also
be ``up`` and running. To see if all of the cluster's OSDs are running, run the following
command:

.. prompt:: bash $

    ceph osd stat

The output provides the following information: the total number of OSDs (x),
how many OSDs are ``up`` (y), how many OSDs are ``in`` (z), and the map epoch (eNNNN). ::

    x osds: y up, z in; epoch: eNNNN

If the number of OSDs that are ``in`` the cluster is greater than the number of
OSDs that are ``up``, run the following command to identify the ``ceph-osd``
daemons that are not running:

.. prompt:: bash $

    ceph osd tree

:: 

    #ID CLASS WEIGHT  TYPE NAME             STATUS REWEIGHT PRI-AFF
     -1       2.00000 pool openstack
     -3       2.00000 rack dell-2950-rack-A
     -2       2.00000 host dell-2950-A1
      0   ssd 1.00000      osd.0                up  1.00000 1.00000
      1   ssd 1.00000      osd.1              down  1.00000 1.00000

.. tip:: Searching through a well-designed CRUSH hierarchy to identify the physical
   locations of particular OSDs might help you troubleshoot your cluster.

If an OSD is ``down``, start it by running the following command:

.. prompt:: bash $

    sudo systemctl start ceph-osd@1

For problems associated with OSDs that have stopped or won't restart, see `OSD Not Running`_.


PG Sets
=======

When CRUSH assigns a PG to OSDs, it takes note of how many replicas of the PG
are required by the pool and then assigns each replica to a different OSD.
For example, if the pool requires three replicas of a PG, CRUSH might assign
them individually to ``osd.1``, ``osd.2`` and ``osd.3``. CRUSH seeks a
pseudo-random placement that takes into account the failure domains that you
have set in your `CRUSH map`_; for this reason, PGs are rarely assigned to
immediately adjacent OSDs in a large cluster.

Ceph processes a client request using the **Acting Set**, which is the set of
OSDs that will actually handle the requests since they have a full and working
version of a placement group shard. The set of OSDs that should contain a shard
of a particular placement group as the **Up Set**, i.e. where data is
moved/copied to (or planned to be).

Sometimes an OSD in the Acting Set is ``down`` or otherwise unable to
service requests for objects in the PG. When this kind of situation
arises, don't panic. Common examples of such a situation include:

- You added or removed an OSD, CRUSH reassigned the PG to 
  other OSDs, and this reassignment changed the composition of the Acting Set and triggered
  the migration of data by means of a "backfill" process.
- An OSD was ``down``, was restarted, and is now ``recovering``.
- An OSD in the Acting Set is ``down`` or unable to service requests,
  and another OSD has temporarily assumed its duties.

Typically, the Up Set and the Acting Set are identical. When they are not, it
might indicate that Ceph is migrating the PG (in other words, that the PG has
been remapped), that an OSD is recovering, or that there is a problem with the
cluster (in such scenarios, Ceph usually shows a "HEALTH WARN" state with a
"stuck stale" message).

To retrieve a list of PGs, run the following command:

.. prompt:: bash $

    ceph pg dump

To see which OSDs are within the Acting Set and the Up Set for a specific PG, run the following command:

.. prompt:: bash $

    ceph pg map {pg-num}

The output provides the following information: the osdmap epoch (eNNN), the PG number
({pg-num}), the OSDs in the Up Set (up[]), and the OSDs in the Acting Set
(acting[])::

    osdmap eNNN pg {raw-pg-num} ({pg-num}) -> up [0,1,2] acting [0,1,2]

.. note:: If the Up Set and the Acting Set do not match, this might indicate
   that the cluster is rebalancing itself or that there is a problem with 
   the cluster.


Peering
=======

Before you can write data to a PG, it must be in an ``active`` state and it
will preferably be in a ``clean`` state. For Ceph to determine the current
state of a PG, peering must take place.  That is, the primary OSD of the PG
(that is, the first OSD in the Acting Set) must peer with the secondary and
OSDs so that consensus on the current state of the PG can be established. In
the following diagram, we assume a pool with three replicas of the PG:

.. ditaa::

           +---------+     +---------+     +-------+
           |  OSD 1  |     |  OSD 2  |     | OSD 3 |
           +---------+     +---------+     +-------+
                |               |              |
                |  Request To   |              |
                |     Peer      |              |
                |-------------->|              |
                |<--------------|              |
                |    Peering                   |
                |                              |
                |         Request To           |
                |            Peer              |
                |----------------------------->|
                |<-----------------------------|
                |          Peering             |

The OSDs also report their status to the monitor. For details, see `Configuring Monitor/OSD
Interaction`_. To troubleshoot peering issues, see `Peering
Failure`_.


Monitoring PG States
====================

If you run the commands ``ceph health``, ``ceph -s``, or ``ceph -w``,
you might notice that the cluster does not always show ``HEALTH OK``. After
first checking to see if the OSDs are running, you should also check PG
states. There are certain PG-peering-related circumstances in which it is expected
and normal that the cluster will **NOT** show ``HEALTH OK``:

#. You have just created a pool and the PGs haven't peered yet.
#. The PGs are recovering.
#. You have just added an OSD to or removed an OSD from the cluster.
#. You have just modified your CRUSH map and your PGs are migrating.
#. There is inconsistent data in different replicas of a PG.
#. Ceph is scrubbing a PG's replicas.
#. Ceph doesn't have enough storage capacity to complete backfilling operations.

If one of these circumstances causes Ceph to show ``HEALTH WARN``, don't
panic. In many cases, the cluster will recover on its own. In some cases, however, you
might need to take action. An important aspect of monitoring PGs is to check their
status as ``active`` and ``clean``: that is, it is important to ensure that, when the
cluster is up and running, all PGs are ``active`` and (preferably) ``clean``.
To see the status of every PG, run the following command:

.. prompt:: bash $

    ceph pg stat

The output provides the following information: the total number of PGs (x), how many
PGs are in a particular state such as ``active+clean`` (y), and the
amount of data stored (z). ::

    x pgs: y active+clean; z bytes data, aa MB used, bb GB / cc GB avail

.. note:: It is common for Ceph to report multiple states for PGs (for example,
   ``active+clean``, ``active+clean+remapped``, ``active+clean+scrubbing``.

Here Ceph shows not only the PG states, but also storage capacity used (aa),
the amount of storage capacity remaining (bb), and the total storage capacity
of the PG. These values can be important in a few cases:

- The cluster is reaching its ``near full ratio`` or ``full ratio``.
- Data is not being distributed across the cluster due to an error in the
  CRUSH configuration.


.. topic:: Placement Group IDs

   PG IDs consist of the pool number (not the pool name) followed 
   by a period (.) and the PG ID-- (a hexadecimal number). You
   can view pool numbers and their names from in the output of ``ceph osd 
   lspools``. For example, the first pool that was created corresponds to
   pool number ``1``. A fully qualified PG ID has the
   following form::

       {pool-num}.{pg-id}

   It typically resembles the following:: 

    1.1701b


To retrieve a list of PGs, run the following command:

.. prompt:: bash $

    ceph pg dump

To format the output in JSON format and save it to a file, run the following command:

.. prompt:: bash $

    ceph pg dump -o {filename} --format=json

To query a specific PG, run the following command:

.. prompt:: bash $

    ceph pg {poolnum}.{pg-id} query

Ceph will output the query in JSON format.

The following subsections describe the most common PG states in detail.


Creating
--------

When you create a pool, it will create the number of placement groups you
specified. Ceph will echo ``creating`` when it is creating one or more
placement groups. Once they are created, the OSDs that are part of a placement
group's Acting Set will peer. Once peering is complete, the placement group
status should be ``active+clean``, which means a Ceph client can begin writing
to the placement group.

.. ditaa::

       /-----------\       /-----------\       /-----------\
       | Creating  |------>|  Peering  |------>|  Active   |
       \-----------/       \-----------/       \-----------/

Peering
-------

When Ceph is Peering a placement group, Ceph is bringing the OSDs that
store the replicas of the placement group into **agreement about the state**
of the objects and metadata in the placement group. When Ceph completes peering,
this means that the OSDs that store the placement group agree about the current
state of the placement group. However, completion of the peering process does
**NOT** mean that each replica has the latest contents.

.. topic:: Authoritative History

   Ceph will **NOT** acknowledge a write operation to a client, until 
   all OSDs of the acting set persist the write operation. This practice 
   ensures that at least one member of the acting set will have a record 
   of every acknowledged write operation since the last successful 
   peering operation.
   
   With an accurate record of each acknowledged write operation, Ceph can 
   construct and disseminate a new authoritative history of the placement 
   group--a complete, and fully ordered set of operations that, if performed, 
   would bring an OSD’s copy of a placement group up to date.


Active
------

Once Ceph completes the peering process, a placement group may become
``active``. The ``active`` state means that the data in the placement group is
generally available in the primary placement group and the replicas for read
and write operations. 


Clean 
-----

When a placement group is in the ``clean`` state, the primary OSD and the
replica OSDs have successfully peered and there are no stray replicas for the
placement group. Ceph replicated all objects in the placement group the correct 
number of times.


Degraded
--------

When a client writes an object to the primary OSD, the primary OSD is
responsible for writing the replicas to the replica OSDs. After the primary OSD
writes the object to storage, the placement group will remain in a ``degraded``
state until the primary OSD has received an acknowledgement from the replica
OSDs that Ceph created the replica objects successfully. 

The reason a placement group can be ``active+degraded`` is that an OSD may be
``active`` even though it doesn't hold all of the objects yet. If an OSD goes
``down``, Ceph marks each placement group assigned to the OSD as ``degraded``.
The OSDs must peer again when the OSD comes back online. However, a client can
still write a new object to a ``degraded`` placement group if it is ``active``.

If an OSD is ``down`` and the ``degraded`` condition persists, Ceph may mark the
``down`` OSD as ``out`` of the cluster and remap the data from the ``down`` OSD
to another OSD. The time between being marked ``down`` and being marked ``out``
is controlled by ``mon_osd_down_out_interval``, which is set to ``600`` seconds
by default.

A placement group can also be ``degraded``, because Ceph cannot find one or more
objects that Ceph thinks should be in the placement group. While you cannot
read or write to unfound objects, you can still access all of the other objects
in the ``degraded`` placement group.


Recovering
----------

Ceph was designed for fault-tolerance at a scale where hardware and software
problems are ongoing. When an OSD goes ``down``, its contents may fall behind
the current state of other replicas in the placement groups. When the OSD is
back ``up``, the contents of the placement groups must be updated to reflect the
current state. During that time period, the OSD may reflect a ``recovering``
state.

Recovery is not always trivial, because a hardware failure might cause a
cascading failure of multiple OSDs. For example, a network switch for a rack or
cabinet may fail, which can cause the OSDs of a number of host machines to fall
behind the current state of the cluster. Each one of the OSDs must recover once
the fault is resolved.

Ceph provides a number of settings to balance the resource contention between
new service requests and the need to recover data objects and restore the
placement groups to the current state. The ``osd_recovery_delay_start`` setting
allows an OSD to restart, re-peer and even process some replay requests before
starting the recovery process. The ``osd_recovery_thread_timeout`` sets a thread
timeout, because multiple OSDs may fail, restart and re-peer at staggered rates.
The ``osd_recovery_max_active`` setting limits the number of recovery requests
an OSD will entertain simultaneously to prevent the OSD from failing to serve.
The ``osd_recovery_max_chunk`` setting limits the size of the recovered data
chunks to prevent network congestion.


Back Filling
------------

When a new OSD joins the cluster, CRUSH will reassign placement groups from OSDs
in the cluster to the newly added OSD. Forcing the new OSD to accept the
reassigned placement groups immediately can put excessive load on the new OSD.
Back filling the OSD with the placement groups allows this process to begin in
the background. Once backfilling is complete, the new OSD will begin serving
requests when it is ready.

During the backfill operations, you may see one of several states:
``backfill_wait`` indicates that a backfill operation is pending, but is not
underway yet; ``backfilling`` indicates that a backfill operation is underway;
and, ``backfill_toofull`` indicates that a backfill operation was requested,
but couldn't be completed due to insufficient storage capacity. When a 
placement group cannot be backfilled, it may be considered ``incomplete``.

The ``backfill_toofull`` state may be transient. It is possible that as PGs
are moved around, space may become available. The ``backfill_toofull`` is
similar to ``backfill_wait`` in that as soon as conditions change
backfill can proceed.

Ceph provides a number of settings to manage the load spike associated with
reassigning placement groups to an OSD (especially a new OSD). By default,
``osd_max_backfills`` sets the maximum number of concurrent backfills to and from
an OSD to 1. The ``backfill_full_ratio`` enables an OSD to refuse a
backfill request if the OSD is approaching its full ratio (90%, by default) and
change with ``ceph osd set-backfillfull-ratio`` command.
If an OSD refuses a backfill request, the ``osd_backfill_retry_interval``
enables an OSD to retry the request (after 30 seconds, by default). OSDs can
also set ``osd_backfill_scan_min`` and ``osd_backfill_scan_max`` to manage scan
intervals (64 and 512, by default).


Remapped
--------

When the Acting Set that services a placement group changes, the data migrates
from the old acting set to the new acting set. It may take some time for a new
primary OSD to service requests. So it may ask the old primary to continue to
service requests until the placement group migration is complete. Once data
migration completes, the mapping uses the primary OSD of the new acting set.


Stale
-----

While Ceph uses heartbeats to ensure that hosts and daemons are running, the
``ceph-osd`` daemons may also get into a ``stuck`` state where they are not
reporting statistics in a timely manner (e.g., a temporary network fault). By
default, OSD daemons report their placement group, up through, boot and failure
statistics every half second (i.e., ``0.5``), which is more frequent than the
heartbeat thresholds. If the **Primary OSD** of a placement group's acting set
fails to report to the monitor or if other OSDs have reported the primary OSD
``down``, the monitors will mark the placement group ``stale``.

When you start your cluster, it is common to see the ``stale`` state until
the peering process completes. After your cluster has been running for awhile, 
seeing placement groups in the ``stale`` state indicates that the primary OSD
for those placement groups is ``down`` or not reporting placement group statistics
to the monitor.


Identifying Troubled PGs
========================

As previously noted, a placement group is not necessarily problematic just 
because its state is not ``active+clean``. Generally, Ceph's ability to self
repair may not be working when placement groups get stuck. The stuck states
include:

- **Unclean**: Placement groups contain objects that are not replicated the 
  desired number of times. They should be recovering.
- **Inactive**: Placement groups cannot process reads or writes because they 
  are waiting for an OSD with the most up-to-date data to come back ``up``.
- **Stale**: Placement groups are in an unknown state, because the OSDs that 
  host them have not reported to the monitor cluster in a while (configured 
  by ``mon_osd_report_timeout``).

To identify stuck placement groups, execute the following:

.. prompt:: bash $

	ceph pg dump_stuck [unclean|inactive|stale|undersized|degraded]

See `Placement Group Subsystem`_ for additional details. To troubleshoot
stuck placement groups, see `Troubleshooting PG Errors`_.


Finding an Object Location
==========================

To store object data in the Ceph Object Store, a Ceph client must: 

#. Set an object name
#. Specify a `pool`_

The Ceph client retrieves the latest cluster map and the CRUSH algorithm
calculates how to map the object to a `placement group`_, and then calculates
how to assign the placement group to an OSD dynamically. To find the object
location, all you need is the object name and the pool name. For example:

.. prompt:: bash $

	ceph osd map {poolname} {object-name} [namespace]

.. topic:: Exercise: Locate an Object

        As an exercise, let's create an object. Specify an object name, a path
        to a test file containing some object data and a pool name using the
        ``rados put`` command on the command line. For example:

        .. prompt:: bash $
   
		rados put {object-name} {file-path} --pool=data
		rados put test-object-1 testfile.txt --pool=data
   
        To verify that the Ceph Object Store stored the object, execute the
        following:
   
        .. prompt:: bash $

           rados -p data ls
   
	Now, identify the object location:

        .. prompt:: bash $

           ceph osd map {pool-name} {object-name}
           ceph osd map data test-object-1
   
	Ceph should output the object's location. For example:: 
   
		osdmap e537 pool 'data' (1) object 'test-object-1' -> pg 1.d1743484 (1.4) -> up ([0,1], p0) acting ([0,1], p0)
   
        To remove the test object, simply delete it using the ``rados rm``
        command.  For example:

        .. prompt:: bash $
   
           rados rm test-object-1 --pool=data
   

As the cluster evolves, the object location may change dynamically. One benefit
of Ceph's dynamic rebalancing is that Ceph relieves you from having to perform
the migration manually. See the `Architecture`_ section for details.

.. _data placement: ../data-placement
.. _pool: ../pools
.. _placement group: ../placement-groups
.. _Architecture: ../../../architecture
.. _OSD Not Running: ../../troubleshooting/troubleshooting-osd#osd-not-running
.. _Troubleshooting PG Errors: ../../troubleshooting/troubleshooting-pg#troubleshooting-pg-errors
.. _Peering Failure: ../../troubleshooting/troubleshooting-pg#failures-osd-peering
.. _CRUSH map: ../crush-map
.. _Configuring Monitor/OSD Interaction: ../../configuration/mon-osd-interaction/
.. _Placement Group Subsystem: ../control#placement-group-subsystem
