Dynamic Cluster Management
==========================

In the :ref:`Scalability and High Availability <arch_scalability_and_high_availability>` section, we explained how Ceph uses
CRUSH, cluster topology, and intelligent daemons to scale and maintain high
availability. Key to Ceph's design is the autonomous, self-healing, and
intelligent Ceph OSD Daemon. Let's take a deeper look at how CRUSH works to
enable modern cloud storage infrastructures to place data, rebalance the
cluster, and adaptively place and balance data and recover from faults.

.. index:: architecture; pools

About Pools
~~~~~~~~~~~

The Ceph storage system supports the notion of 'Pools', which are logical
partitions for storing objects.

Ceph Clients retrieve a :ref:`Cluster Map <architecture_cluster_map>` from a Ceph Monitor, and write RADOS
objects to pools. The way that Ceph places the data in the pools is determined
by the pool's ``size`` or number of replicas, the CRUSH rule, and the number of
placement groups in the pool.

.. ditaa::

            +--------+  Retrieves  +---------------+
            | Client |------------>|  Cluster Map  |
            +--------+             +---------------+
                 |
                 v      Writes
              /-----\
              | obj |
              \-----/
                 |      To
                 v
            +--------+           +---------------+
            |  Pool  |---------->|  CRUSH Rule   |
            +--------+  Selects  +---------------+


Pools set at least the following parameters:

- Ownership/Access to Objects
- The Number of Placement Groups, and
- The CRUSH Rule to Use.

See :ref:`setpoolvalues` for details.


.. index:: architecture; placement group mapping

Mapping PGs to OSDs
~~~~~~~~~~~~~~~~~~~

Each pool has a number of placement groups (PGs) within it. CRUSH dynamically
maps PGs to OSDs. When a Ceph Client stores objects, CRUSH maps each RADOS
object to a PG.

This mapping of RADOS objects to PGs implements an abstraction and indirection
layer between Ceph OSD Daemons and Ceph Clients. The Ceph Storage Cluster must
be able to grow (or shrink) and redistribute data adaptively when the internal
topology changes.

If the Ceph Client "knew" which Ceph OSD Daemons were storing which objects, a
tight coupling would exist between the Ceph Client and the Ceph OSD Daemon.
But Ceph avoids any such tight coupling. Instead, the CRUSH algorithm maps each
RADOS object to a placement group and then maps each placement group to one or
more Ceph OSD Daemons. This "layer of indirection" allows Ceph to rebalance
dynamically when new Ceph OSD Daemons and their underlying OSD devices come
online. The following diagram shows how the CRUSH algorithm maps objects to
placement groups, and how it maps placement groups to OSDs.

.. ditaa::

           /-----\  /-----\  /-----\  /-----\  /-----\
           | obj |  | obj |  | obj |  | obj |  | obj |
           \-----/  \-----/  \-----/  \-----/  \-----/
              |        |        |        |        |
              +--------+--------+        +---+----+
              |                              |
              v                              v
   +-----------------------+      +-----------------------+
   |  Placement Group #1   |      |  Placement Group #2   |
   |                       |      |                       |
   +-----------------------+      +-----------------------+
               |                              |
               |      +-----------------------+---+
        +------+------+-------------+             |
        |             |             |             |
        v             v             v             v
   /----------\  /----------\  /----------\  /----------\
   |          |  |          |  |          |  |          |
   |  OSD #1  |  |  OSD #2  |  |  OSD #3  |  |  OSD #4  |
   |          |  |          |  |          |  |          |
   \----------/  \----------/  \----------/  \----------/

The client uses its copy of the cluster map and the CRUSH algorithm to compute
precisely which OSD it will use when reading or writing a particular object.

.. index:: architecture; calculating PG IDs

Calculating PG IDs
~~~~~~~~~~~~~~~~~~

When a Ceph Client binds to a Ceph Monitor, it retrieves the latest version of
the :ref:`Cluster Map <architecture_cluster_map>`. When a client has been equipped with a copy of the cluster
map, it is aware of all the monitors, OSDs, and metadata servers in the
cluster. **However, even equipped with a copy of the latest version of the
cluster map, the client doesn't know anything about object locations.**

**Object locations must be computed.**

The client requires only the object ID and the name of the pool in order to
compute the object location.

Ceph stores data in named pools (for example,  "liverpool"). When a client
stores a named object (for example, "john", "paul", "george", or "ringo") it
calculates a placement group by using the object name, a hash code, the number
of PGs in the pool, and the pool name. Ceph clients use the following steps to
compute PG IDs.

#. The client inputs the pool name and the object ID. (for example: pool =
   "liverpool" and object-id = "john")
#. Ceph hashes the object ID.
#. Ceph calculates the hash, modulo the number of PGs (for example: ``58``), to
   get a PG ID.
#. Ceph uses the pool name to retrieve the pool ID: (for example: "liverpool" =
   ``4``)
#. Ceph prepends the pool ID to the PG ID (for example: ``4.58``).

It is much faster to compute object locations than to perform object location
query over a chatty session. The :abbr:`CRUSH (Controlled Replication Under
Scalable Hashing)` algorithm allows a client to compute where objects are
expected to be stored, and enables the client to contact the primary OSD to
store or retrieve the objects.

.. index:: architecture; PG Peering

Peering and Sets
~~~~~~~~~~~~~~~~

In previous sections, we noted that Ceph OSD Daemons check each other's
heartbeats and report back to Ceph Monitors. Ceph OSD daemons also 'peer',
which is the process of bringing all of the OSDs that store a Placement Group
(PG) into agreement about the state of all of the RADOS objects (and their
metadata) in that PG. Ceph OSD Daemons `Report Peering Failure`_ to the Ceph
Monitors. Peering issues usually resolve themselves; however, if the problem
persists, you may need to refer to the :ref:`Troubleshooting Peering Failure <failures-osd-peering>`
section.

.. Note:: PGs that agree on the state of the cluster do not necessarily have
   the current data yet.

The Ceph Storage Cluster was designed to store at least two copies of an object
(that is, ``size = 2``), which is the minimum requirement for data safety. For
high availability, a Ceph Storage Cluster should store more than two copies of
an object (that is, ``size = 3`` and ``min size = 2``) so that it can continue
to run in a ``degraded`` state while maintaining data safety.

.. warning:: Although we say here that R2 (replication with two copies) is the
   minimum requirement for data safety, R3 (replication with three copies) is
   recommended. On a long enough timeline, data stored with an R2 strategy will
   be lost.

As explained in the diagram in :ref:`Smart Daemons Enable Hyperscale <arch_smart_daemons>`, we do not
name the Ceph OSD Daemons specifically (for example, ``osd.0``, ``osd.1``,
etc.), but rather refer to them as *Primary*, *Secondary*, and so forth. By
convention, the *Primary* is the first OSD in the *Acting Set*, and is
responsible for orchestrating the peering process for each placement group
where it acts as the *Primary*. The *Primary* is the **ONLY** OSD in a given
placement group that accepts client-initiated writes to objects.

The set of OSDs that is responsible for a placement group is called the
*Acting Set*. The term "*Acting Set*" can refer either to the Ceph OSD Daemons
that are currently responsible for the placement group, or to the Ceph OSD
Daemons that were responsible for a particular placement group as of some
epoch.

The Ceph OSD daemons that are part of an *Acting Set* might not always be
``up``. When an OSD in the *Acting Set* is ``up``, it is part of the *Up Set*.
The *Up Set* is an important distinction, because Ceph can remap PGs to other
Ceph OSD Daemons when an OSD fails.

.. note:: Consider a hypothetical *Acting Set* for a PG that contains
   ``osd.25``, ``osd.32`` and ``osd.61``. The first OSD (``osd.25``), is the
   *Primary*. If that OSD fails, the Secondary (``osd.32``), becomes the
   *Primary*, and ``osd.25`` is removed from the *Up Set*.

.. index:: architecture; Rebalancing

Rebalancing
~~~~~~~~~~~

When you add a Ceph OSD Daemon to a Ceph Storage Cluster, the cluster map gets
updated with the new OSD. Referring back to `Calculating PG IDs`_, this changes
the cluster map. Consequently, it changes object placement, because it changes
an input for the calculations. The following diagram depicts the rebalancing
process (albeit rather crudely, since it is substantially less impactful with
large clusters) where some, but not all of the PGs migrate from existing OSDs
(OSD 1, and OSD 2) to the new OSD (OSD 3). Even when rebalancing, CRUSH is
stable. Many of the placement groups remain in their original configuration,
and each OSD gets some added capacity, so there are no load spikes on the
new OSD after rebalancing is complete.


.. ditaa::

           +--------+     +--------+
   Before  |  OSD 1 |     |  OSD 2 |
           +--------+     +--------+
           |  PG #1 |     | PG #6  |
           |  PG #2 |     | PG #7  |
           |  PG #3 |     | PG #8  |
           |  PG #4 |     | PG #9  |
           |  PG #5 |     | PG #10 |
           +--------+     +--------+

           +--------+     +--------+     +--------+
    After  |  OSD 1 |     |  OSD 2 |     |  OSD 3 |
           +--------+     +--------+     +--------+
           |  PG #1 |     | PG #7  |     |  PG #3 |
           |  PG #2 |     | PG #8  |     |  PG #6 |
           |  PG #4 |     | PG #10 |     |  PG #9 |
           |  PG #5 |     |        |     |        |
           |        |     |        |     |        |
           +--------+     +--------+     +--------+


.. index:: architecture; Data Scrubbing

Data Consistency
~~~~~~~~~~~~~~~~

As part of maintaining data consistency and cleanliness, Ceph OSDs also scrub
objects within placement groups. That is, Ceph OSDs compare object metadata in
one placement group with its replicas in placement groups stored in other
OSDs. Scrubbing (usually performed daily) catches OSD bugs or filesystem
errors, often as a result of hardware issues.  OSDs also perform deeper
scrubbing by comparing data in objects bit-for-bit.  Deep scrubbing (by default
performed weekly) finds bad blocks on a drive that weren't apparent in a light
scrub.

See :ref:`Data Scrubbing <rados_config_scrubbing>` for details on configuring scrubbing.


.. _Report Peering Failure: ../../rados/configuration/mon-osd-interaction#osds-report-peering-failure
