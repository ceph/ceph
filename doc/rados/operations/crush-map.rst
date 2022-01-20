============
 CRUSH Maps
============

The :abbr:`CRUSH (Controlled Replication Under Scalable Hashing)` algorithm
determines how to store and retrieve data by computing storage locations.
CRUSH empowers Ceph clients to communicate with OSDs directly rather than
through a centralized server or broker. With an algorithmically determined
method of storing and retrieving data, Ceph avoids a single point of failure, a
performance bottleneck, and a physical limit to its scalability.

CRUSH uses a map of your cluster (the CRUSH map) to pseudo-randomly
map data to OSDs, distributing it across the cluster according to configured
replication policy and failure domain.  For a detailed discussion of CRUSH, see
`CRUSH - Controlled, Scalable, Decentralized Placement of Replicated Data`_

CRUSH maps contain a list of :abbr:`OSDs (Object Storage Devices)`, a hierarchy
of 'buckets' for aggregating devices and buckets, and
rules that govern how CRUSH replicates data within the cluster's pools. By
reflecting the underlying physical organization of the installation, CRUSH can
model (and thereby address) the potential for correlated device failures.
Typical factors include chassis, racks, physical proximity, a shared power
source, and shared networking. By encoding this information into the cluster
map, CRUSH placement
policies distribute object replicas across failure domains while
maintaining the desired distribution. For example, to address the
possibility of concurrent failures, it may be desirable to ensure that data
replicas are on devices using different shelves, racks, power supplies,
controllers, and/or physical locations.

When you deploy OSDs they are automatically added to the CRUSH map under a
``host`` bucket named for the node on which they run.  This,
combined with the configured CRUSH failure domain, ensures that replicas or
erasure code shards are distributed across hosts and that a single host or other
failure will not affect availability.  For larger clusters, administrators must
carefully consider their choice of failure domain.  Separating replicas across racks,
for example, is typical for mid- to large-sized clusters.


CRUSH Location
==============

The location of an OSD within the CRUSH map's hierarchy is
referred to as a ``CRUSH location``.  This location specifier takes the
form of a list of key and value pairs.  For
example, if an OSD is in a particular row, rack, chassis and host, and
is part of the 'default' CRUSH root (which is the case for most
clusters), its CRUSH location could be described as::

  root=default row=a rack=a2 chassis=a2a host=a2a1

Note:

#. Note that the order of the keys does not matter.
#. The key name (left of ``=``) must be a valid CRUSH ``type``.  By default
   these include ``root``, ``datacenter``, ``room``, ``row``, ``pod``, ``pdu``,
   ``rack``, ``chassis`` and ``host``.
   These defined types suffice for almost all clusters, but can be customized
   by modifying the CRUSH map.
#. Not all keys need to be specified.  For example, by default, Ceph
   automatically sets an ``OSD``'s location to be
   ``root=default host=HOSTNAME`` (based on the output from ``hostname -s``).

The CRUSH location for an OSD can be defined by adding the ``crush location``
option in ``ceph.conf``.  Each time the OSD starts,
it verifies it is in the correct location in the CRUSH map and, if it is not,
it moves itself.  To disable this automatic CRUSH map management, add the
following to your configuration file in the ``[osd]`` section::

  osd crush update on start = false

Note that in most cases you will not need to manually configure this.


Custom location hooks
---------------------

A customized location hook can be used to generate a more complete
CRUSH location on startup.  The CRUSH location is based on, in order
of preference:

#. A ``crush location`` option in ``ceph.conf``
#. A default of ``root=default host=HOSTNAME`` where the hostname is
   derived from the ``hostname -s`` command

A script can be written to provide additional
location fields (for example, ``rack`` or ``datacenter``) and the
hook enabled via the config option::

 crush location hook = /path/to/customized-ceph-crush-location

This hook is passed several arguments (below) and should output a single line
to ``stdout`` with the CRUSH location description.::

  --cluster CLUSTER --id ID --type TYPE

where the cluster name is typically ``ceph``, the ``id`` is the daemon
identifier (e.g., the OSD number or daemon identifier), and the daemon
type is ``osd``, ``mds``, etc.

For example, a simple hook that additionally specifies a rack location
based on a value in the file ``/etc/rack`` might be::

  #!/bin/sh
  echo "host=$(hostname -s) rack=$(cat /etc/rack) root=default"


CRUSH structure
===============

The CRUSH map consists of a hierarchy that describes
the physical topology of the cluster and a set of rules defining
data placement policy.  The hierarchy has
devices (OSDs) at the leaves, and internal nodes
corresponding to other physical features or groupings: hosts, racks,
rows, datacenters, and so on.  The rules describe how replicas are
placed in terms of that hierarchy (e.g., 'three replicas in different
racks').

Devices
-------

Devices are individual OSDs that store data, usually one for each storage drive.
Devices are identified by an ``id``
(a non-negative integer) and a ``name``, normally ``osd.N`` where ``N`` is the device id.

Since the Luminous release, devices may also have a *device class* assigned (e.g.,
``hdd`` or ``ssd`` or ``nvme``), allowing them to be conveniently targeted by
CRUSH rules.  This is especially useful when mixing device types within hosts.

.. _crush_map_default_types:

Types and Buckets
-----------------

A bucket is the CRUSH term for internal nodes in the hierarchy: hosts,
racks, rows, etc.  The CRUSH map defines a series of *types* that are
used to describe these nodes.  Default types include:

- ``osd`` (or ``device``)
- ``host``
- ``chassis``
- ``rack``
- ``row``
- ``pdu``
- ``pod``
- ``room``
- ``datacenter``
- ``zone``
- ``region``
- ``root``

Most clusters use only a handful of these types, and others
can be defined as needed.

The hierarchy is built with devices (normally type ``osd``) at the
leaves, interior nodes with non-device types, and a root node of type
``root``.  For example,

.. ditaa::

                        +-----------------+
                        |{o}root default  |
                        +--------+--------+
                                 |
                 +---------------+---------------+
                 |                               |
          +------+------+                 +------+------+
          |{o}host foo  |                 |{o}host bar  |
          +------+------+                 +------+------+
                 |                               |
         +-------+-------+               +-------+-------+
         |               |               |               |
   +-----+-----+   +-----+-----+   +-----+-----+   +-----+-----+
   |   osd.0   |   |   osd.1   |   |   osd.2   |   |   osd.3   |
   +-----------+   +-----------+   +-----------+   +-----------+

Each node (device or bucket) in the hierarchy has a *weight*
that indicates the relative proportion of the total
data that device or hierarchy subtree should store.  Weights are set
at the leaves, indicating the size of the device, and automatically
sum up the tree, such that the weight of the ``root`` node
will be the total of all devices contained beneath it.  Normally
weights are in units of terabytes (TB).

You can get a simple view the of CRUSH hierarchy for your cluster,
including weights, with::

  ceph osd tree

Rules
-----

CRUSH Rules define policy about how data is distributed across the devices
in the hierarchy. They define placement and replication strategies or
distribution policies that allow you to specify exactly how CRUSH
places data replicas. For example, you might create a rule selecting
a pair of targets for two-way mirroring, another rule for selecting
three targets in two different data centers for three-way mirroring, and
yet another rule for erasure coding (EC) across six storage devices. For a
detailed discussion of CRUSH rules, refer to `CRUSH - Controlled,
Scalable, Decentralized Placement of Replicated Data`_, and more
specifically to **Section 3.2**.

CRUSH rules can be created via the CLI by
specifying the *pool type* they will be used for (replicated or
erasure coded), the *failure domain*, and optionally a *device class*.
In rare cases rules must be written by hand by manually editing the
CRUSH map.

You can see what rules are defined for your cluster with::

  ceph osd crush rule ls

You can view the contents of the rules with::

  ceph osd crush rule dump

Device classes
--------------

Each device can optionally have a *class* assigned.  By
default, OSDs automatically set their class at startup to
`hdd`, `ssd`, or `nvme` based on the type of device they are backed
by.

The device class for one or more OSDs can be explicitly set with::

  ceph osd crush set-device-class <class> <osd-name> [...]

Once a device class is set, it cannot be changed to another class
until the old class is unset with::

  ceph osd crush rm-device-class <osd-name> [...]

This allows administrators to set device classes without the class
being changed on OSD restart or by some other script.

A placement rule that targets a specific device class can be created with::

  ceph osd crush rule create-replicated <rule-name> <root> <failure-domain> <class>

A pool can then be changed to use the new rule with::

  ceph osd pool set <pool-name> crush_rule <rule-name>

Device classes are implemented by creating a "shadow" CRUSH hierarchy
for each device class in use that contains only devices of that class.
CRUSH rules can then distribute data over the shadow hierarchy.
This approach is fully backward compatible with
old Ceph clients.  You can view the CRUSH hierarchy with shadow items
with::

  ceph osd crush tree --show-shadow

For older clusters created before Luminous that relied on manually
crafted CRUSH maps to maintain per-device-type hierarchies, there is a
*reclassify* tool available to help transition to device classes
without triggering data movement (see :ref:`crush-reclassify`).


Weights sets
------------

A *weight set* is an alternative set of weights to use when
calculating data placement.  The normal weights associated with each
device in the CRUSH map are set based on the device size and indicate
how much data we *should* be storing where.  However, because CRUSH is
a "probabilistic" pseudorandom placement process, there is always some
variation from this ideal distribution, in the same way that rolling a
die sixty times will not result in rolling exactly 10 ones and 10
sixes.  Weight sets allow the cluster to perform numerical optimization
based on the specifics of your cluster (hierarchy, pools, etc.) to achieve
a balanced distribution.

There are two types of weight sets supported:

 #. A **compat** weight set is a single alternative set of weights for
    each device and node in the cluster.  This is not well-suited for
    correcting for all anomalies (for example, placement groups for
    different pools may be different sizes and have different load
    levels, but will be mostly treated the same by the balancer).
    However, compat weight sets have the huge advantage that they are
    *backward compatible* with previous versions of Ceph, which means
    that even though weight sets were first introduced in Luminous
    v12.2.z, older clients (e.g., firefly) can still connect to the
    cluster when a compat weight set is being used to balance data.
 #. A **per-pool** weight set is more flexible in that it allows
    placement to be optimized for each data pool.  Additionally,
    weights can be adjusted for each position of placement, allowing
    the optimizer to correct for a subtle skew of data toward devices
    with small weights relative to their peers (and effect that is
    usually only apparently in very large clusters but which can cause
    balancing problems).

When weight sets are in use, the weights associated with each node in
the hierarchy is visible as a separate column (labeled either
``(compat)`` or the pool name) from the command::

  ceph osd tree

When both *compat* and *per-pool* weight sets are in use, data
placement for a particular pool will use its own per-pool weight set
if present.  If not, it will use the compat weight set if present.  If
neither are present, it will use the normal CRUSH weights.

Although weight sets can be set up and manipulated by hand, it is
recommended that the ``ceph-mgr`` *balancer* module be enabled to do so
automatically when running Luminous or later releases.


Modifying the CRUSH map
=======================

.. _addosd:

Add/Move an OSD
---------------

.. note: OSDs are normally automatically added to the CRUSH map when
         the OSD is created.  This command is rarely needed.

To add or move an OSD in the CRUSH map of a running cluster::

  ceph osd crush set {name} {weight} root={root} [{bucket-type}={bucket-name} ...]

Where:

``name``

:Description: The full name of the OSD.
:Type: String
:Required: Yes
:Example: ``osd.0``


``weight``

:Description: The CRUSH weight for the OSD, normally its size measure in terabytes (TB).
:Type: Double
:Required: Yes
:Example: ``2.0``


``root``

:Description: The root node of the tree in which the OSD resides (normally ``default``)
:Type: Key/value pair.
:Required: Yes
:Example: ``root=default``


``bucket-type``

:Description: You may specify the OSD's location in the CRUSH hierarchy.
:Type: Key/value pairs.
:Required: No
:Example: ``datacenter=dc1 room=room1 row=foo rack=bar host=foo-bar-1``


The following example adds ``osd.0`` to the hierarchy, or moves the
OSD from a previous location. ::

  ceph osd crush set osd.0 1.0 root=default datacenter=dc1 room=room1 row=foo rack=bar host=foo-bar-1


Adjust OSD weight
-----------------

.. note: Normally OSDs automatically add themselves to the CRUSH map
         with the correct weight when they are created. This command
         is rarely needed.

To adjust an OSD's CRUSH weight in the CRUSH map of a running cluster, execute
the following::

  ceph osd crush reweight {name} {weight}

Where:

``name``

:Description: The full name of the OSD.
:Type: String
:Required: Yes
:Example: ``osd.0``


``weight``

:Description: The CRUSH weight for the OSD.
:Type: Double
:Required: Yes
:Example: ``2.0``


.. _removeosd:

Remove an OSD
-------------

.. note: OSDs are normally removed from the CRUSH as part of the
   ``ceph osd purge`` command.  This command is rarely needed.

To remove an OSD from the CRUSH map of a running cluster, execute the
following::

  ceph osd crush remove {name}

Where:

``name``

:Description: The full name of the OSD.
:Type: String
:Required: Yes
:Example: ``osd.0``


Add a Bucket
------------

.. note: Buckets are implicitly created when an OSD is added
   that specifies a ``{bucket-type}={bucket-name}`` as part of its
   location,  if a bucket with that name does not already exist.  This
   command is typically used when manually adjusting the structure of the
   hierarchy after OSDs have been created.  One use is to move a
   series of hosts underneath a new rack-level bucket; another is to
   add new ``host`` buckets (OSD nodes) to a dummy ``root`` so that they don't
   receive data until you're ready, at which time you would move them to the
   ``default`` or other root as described below.

To add a bucket in the CRUSH map of a running cluster, execute the
``ceph osd crush add-bucket`` command::

  ceph osd crush add-bucket {bucket-name} {bucket-type}

Where:

``bucket-name``

:Description: The full name of the bucket.
:Type: String
:Required: Yes
:Example: ``rack12``


``bucket-type``

:Description: The type of the bucket. The type must already exist in the hierarchy.
:Type: String
:Required: Yes
:Example: ``rack``


The following example adds the ``rack12`` bucket to the hierarchy::

  ceph osd crush add-bucket rack12 rack

Move a Bucket
-------------

To move a bucket to a different location or position in the CRUSH map
hierarchy, execute the following::

  ceph osd crush move {bucket-name} {bucket-type}={bucket-name}, [...]

Where:

``bucket-name``

:Description: The name of the bucket to move/reposition.
:Type: String
:Required: Yes
:Example: ``foo-bar-1``

``bucket-type``

:Description: You may specify the bucket's location in the CRUSH hierarchy.
:Type: Key/value pairs.
:Required: No
:Example: ``datacenter=dc1 room=room1 row=foo rack=bar host=foo-bar-1``

Remove a Bucket
---------------

To remove a bucket from the CRUSH hierarchy, execute the following::

  ceph osd crush remove {bucket-name}

.. note:: A bucket must be empty before removing it from the CRUSH hierarchy.

Where:

``bucket-name``

:Description: The name of the bucket that you'd like to remove.
:Type: String
:Required: Yes
:Example: ``rack12``

The following example removes the ``rack12`` bucket from the hierarchy::

  ceph osd crush remove rack12

Creating a compat weight set
----------------------------

.. note: This step is normally done automatically by the ``balancer``
   module when enabled.

To create a *compat* weight set::

  ceph osd crush weight-set create-compat

Weights for the compat weight set can be adjusted with::

  ceph osd crush weight-set reweight-compat {name} {weight}

The compat weight set can be destroyed with::

  ceph osd crush weight-set rm-compat

Creating per-pool weight sets
-----------------------------

To create a weight set for a specific pool,::

  ceph osd crush weight-set create {pool-name} {mode}

.. note:: Per-pool weight sets require that all servers and daemons
          run Luminous v12.2.z or later.

Where:

``pool-name``

:Description: The name of a RADOS pool
:Type: String
:Required: Yes
:Example: ``rbd``

``mode``

:Description: Either ``flat`` or ``positional``.  A *flat* weight set
	      has a single weight for each device or bucket.  A
	      *positional* weight set has a potentially different
	      weight for each position in the resulting placement
	      mapping.  For example, if a pool has a replica count of
	      3, then a positional weight set will have three weights
	      for each device and bucket.
:Type: String
:Required: Yes
:Example: ``flat``

To adjust the weight of an item in a weight set::

  ceph osd crush weight-set reweight {pool-name} {item-name} {weight [...]}

To list existing weight sets,::

  ceph osd crush weight-set ls

To remove a weight set,::

  ceph osd crush weight-set rm {pool-name}

Creating a rule for a replicated pool
-------------------------------------

For a replicated pool, the primary decision when creating the CRUSH
rule is what the failure domain is going to be.  For example, if a
failure domain of ``host`` is selected, then CRUSH will ensure that
each replica of the data is stored on a unique host.  If ``rack``
is selected, then each replica will be stored in a different rack.
What failure domain you choose primarily depends on the size and
topology of your cluster.

In most cases the entire cluster hierarchy is nested beneath a root node
named ``default``.  If you have customized your hierarchy, you may
want to create a rule nested at some other node in the hierarchy.  It
doesn't matter what type is associated with that node (it doesn't have
to be a ``root`` node).

It is also possible to create a rule that restricts data placement to
a specific *class* of device.  By default, Ceph OSDs automatically
classify themselves as either ``hdd`` or ``ssd``, depending on the
underlying type of device being used.  These classes can also be
customized.

To create a replicated rule,::

  ceph osd crush rule create-replicated {name} {root} {failure-domain-type} [{class}]

Where:

``name``

:Description: The name of the rule
:Type: String
:Required: Yes
:Example: ``rbd-rule``

``root``

:Description: The name of the node under which data should be placed.
:Type: String
:Required: Yes
:Example: ``default``

``failure-domain-type``

:Description: The type of CRUSH nodes across which we should separate replicas.
:Type: String
:Required: Yes
:Example: ``rack``

``class``

:Description: The device class on which data should be placed.
:Type: String
:Required: No
:Example: ``ssd``

Creating a rule for an erasure coded pool
-----------------------------------------

For an erasure-coded (EC) pool, the same basic decisions need to be made:
what is the failure domain, which node in the
hierarchy will data be placed under (usually ``default``), and will
placement be restricted to a specific device class.  Erasure code
pools are created a bit differently, however, because they need to be
constructed carefully based on the erasure code being used.  For this reason,
you must include this information in the *erasure code profile*.  A CRUSH
rule will then be created from that either explicitly or automatically when
the profile is used to create a pool.

The erasure code profiles can be listed with::

  ceph osd erasure-code-profile ls

An existing profile can be viewed with::

  ceph osd erasure-code-profile get {profile-name}

Normally profiles should never be modified; instead, a new profile
should be created and used when creating a new pool or creating a new
rule for an existing pool.

An erasure code profile consists of a set of key=value pairs.  Most of
these control the behavior of the erasure code that is encoding data
in the pool.  Those that begin with ``crush-``, however, affect the
CRUSH rule that is created.

The erasure code profile properties of interest are:

 * **crush-root**: the name of the CRUSH node under which to place data [default: ``default``].
 * **crush-failure-domain**: the CRUSH bucket type across which to distribute erasure-coded shards [default: ``host``].
 * **crush-device-class**: the device class on which to place data [default: none, meaning all devices are used].
 * **k** and **m** (and, for the ``lrc`` plugin, **l**): these determine the number of erasure code shards, affecting the resulting CRUSH rule.

Once a profile is defined, you can create a CRUSH rule with::

  ceph osd crush rule create-erasure {name} {profile-name}

.. note: When creating a new pool, it is not actually necessary to
   explicitly create the rule.  If the erasure code profile alone is
   specified and the rule argument is left off then Ceph will create
   the CRUSH rule automatically.

Deleting rules
--------------

Rules that are not in use by pools can be deleted with::

  ceph osd crush rule rm {rule-name}


.. _crush-map-tunables:

Tunables
========

Over time, we have made (and continue to make) improvements to the
CRUSH algorithm used to calculate the placement of data.  In order to
support the change in behavior, we have introduced a series of tunable
options that control whether the legacy or improved variation of the
algorithm is used.

In order to use newer tunables, both clients and servers must support
the new version of CRUSH.  For this reason, we have created
``profiles`` that are named after the Ceph version in which they were
introduced.  For example, the ``firefly`` tunables are first supported
by the Firefly release, and will not work with older (e.g., Dumpling)
clients.  Once a given set of tunables are changed from the legacy
default behavior, the ``ceph-mon`` and ``ceph-osd`` will prevent older
clients who do not support the new CRUSH features from connecting to
the cluster.

argonaut (legacy)
-----------------

The legacy CRUSH behavior used by Argonaut and older releases works
fine for most clusters, provided there are not many OSDs that have
been marked out.

bobtail (CRUSH_TUNABLES2)
-------------------------

The ``bobtail`` tunable profile fixes a few key misbehaviors:

 * For hierarchies with a small number of devices in the leaf buckets,
   some PGs map to fewer than the desired number of replicas.  This
   commonly happens for hierarchies with "host" nodes with a small
   number (1-3) of OSDs nested beneath each one.

 * For large clusters, some small percentages of PGs map to fewer than
   the desired number of OSDs.  This is more prevalent when there are
   multiple hierarchy layers in use (e.g., ``row``, ``rack``, ``host``, ``osd``).

 * When some OSDs are marked out, the data tends to get redistributed
   to nearby OSDs instead of across the entire hierarchy.

The new tunables are:

 * ``choose_local_tries``: Number of local retries.  Legacy value is
   2, optimal value is 0.

 * ``choose_local_fallback_tries``: Legacy value is 5, optimal value
   is 0.

 * ``choose_total_tries``: Total number of attempts to choose an item.
   Legacy value was 19, subsequent testing indicates that a value of
   50 is more appropriate for typical clusters.  For extremely large
   clusters, a larger value might be necessary.

 * ``chooseleaf_descend_once``: Whether a recursive chooseleaf attempt
   will retry, or only try once and allow the original placement to
   retry.  Legacy default is 0, optimal value is 1.

Migration impact:

 * Moving from ``argonaut`` to ``bobtail`` tunables triggers a moderate amount
   of data movement.  Use caution on a cluster that is already
   populated with data.

firefly (CRUSH_TUNABLES3)
-------------------------

The ``firefly`` tunable profile fixes a problem
with ``chooseleaf`` CRUSH rule behavior that tends to result in PG
mappings with too few results when too many OSDs have been marked out.

The new tunable is:

 * ``chooseleaf_vary_r``: Whether a recursive chooseleaf attempt will
   start with a non-zero value of ``r``, based on how many attempts the
   parent has already made.  Legacy default is ``0``, but with this value
   CRUSH is sometimes unable to find a mapping.  The optimal value (in
   terms of computational cost and correctness) is ``1``.

Migration impact:

 * For existing clusters that house lots of data, changing
   from ``0`` to ``1`` will cause a lot of data to move; a value of ``4`` or ``5``
   will allow CRUSH to still find a valid mapping but will cause less data
   to move.

straw_calc_version tunable (introduced with Firefly too)
--------------------------------------------------------

There were some problems with the internal weights calculated and
stored in the CRUSH map for ``straw`` algorithm buckets.  Specifically, when
there were items with a CRUSH weight of ``0``, or both a mix of different and
unique weights, CRUSH would distribute data incorrectly (i.e.,
not in proportion to the weights).

The new tunable is:

 * ``straw_calc_version``: A value of ``0`` preserves the old, broken
   internal weight calculation; a value of ``1`` fixes the behavior.

Migration impact:

 * Moving to straw_calc_version ``1`` and then adjusting a straw bucket
   (by adding, removing, or reweighting an item, or by using the
   reweight-all command) can trigger a small to moderate amount of
   data movement *if* the cluster has hit one of the problematic
   conditions.

This tunable option is special because it has absolutely no impact
concerning the required kernel version in the client side.

hammer (CRUSH_V4)
-----------------

The ``hammer`` tunable profile does not affect the
mapping of existing CRUSH maps simply by changing the profile.  However:

 * There is a new bucket algorithm (``straw2``) supported.  The new
   ``straw2`` bucket algorithm fixes several limitations in the original
   ``straw``.  Specifically, the old ``straw`` buckets would
   change some mappings that should have changed when a weight was
   adjusted, while ``straw2`` achieves the original goal of only
   changing mappings to or from the bucket item whose weight has
   changed.

 * ``straw2`` is the default for any newly created buckets.

Migration impact:

 * Changing a bucket type from ``straw`` to ``straw2`` will result in
   a reasonably small amount of data movement, depending on how much
   the bucket item weights vary from each other.  When the weights are
   all the same no data will move, and when item weights vary
   significantly there will be more movement.

jewel (CRUSH_TUNABLES5)
-----------------------

The ``jewel`` tunable profile improves the
overall behavior of CRUSH such that significantly fewer mappings
change when an OSD is marked out of the cluster.  This results in
significantly less data movement.

The new tunable is:

 * ``chooseleaf_stable``: Whether a recursive chooseleaf attempt will
   use a better value for an inner loop that greatly reduces the number
   of mapping changes when an OSD is marked out.  The legacy value is ``0``,
   while the new value of ``1`` uses the new approach.

Migration impact:

 * Changing this value on an existing cluster will result in a very
   large amount of data movement as almost every PG mapping is likely
   to change.




Which client versions support CRUSH_TUNABLES
--------------------------------------------

 * argonaut series, v0.48.1 or later
 * v0.49 or later
 * Linux kernel version v3.6 or later (for the file system and RBD kernel clients)

Which client versions support CRUSH_TUNABLES2
---------------------------------------------

 * v0.55 or later, including bobtail series (v0.56.x)
 * Linux kernel version v3.9 or later (for the file system and RBD kernel clients)

Which client versions support CRUSH_TUNABLES3
---------------------------------------------

 * v0.78 (firefly) or later
 * Linux kernel version v3.15 or later (for the file system and RBD kernel clients)

Which client versions support CRUSH_V4
--------------------------------------

 * v0.94 (hammer) or later
 * Linux kernel version v4.1 or later (for the file system and RBD kernel clients)

Which client versions support CRUSH_TUNABLES5
---------------------------------------------

 * v10.0.2 (jewel) or later
 * Linux kernel version v4.5 or later (for the file system and RBD kernel clients)

Warning when tunables are non-optimal
-------------------------------------

Starting with version v0.74, Ceph will issue a health warning if the
current CRUSH tunables don't include all the optimal values from the
``default`` profile (see below for the meaning of the ``default`` profile).
To make this warning go away, you have two options:

1. Adjust the tunables on the existing cluster.  Note that this will
   result in some data movement (possibly as much as 10%).  This is the
   preferred route, but should be taken with care on a production cluster
   where the data movement may affect performance.  You can enable optimal
   tunables with::

      ceph osd crush tunables optimal

   If things go poorly (e.g., too much load) and not very much
   progress has been made, or there is a client compatibility problem
   (old kernel CephFS or RBD clients, or pre-Bobtail ``librados``
   clients), you can switch back with::

      ceph osd crush tunables legacy

2. You can make the warning go away without making any changes to CRUSH by
   adding the following option to your ceph.conf ``[mon]`` section::

      mon warn on legacy crush tunables = false

   For the change to take effect, you will need to restart the monitors, or
   apply the option to running monitors with::

      ceph tell mon.\* config set mon_warn_on_legacy_crush_tunables false


A few important points
----------------------

 * Adjusting these values will result in the shift of some PGs between
   storage nodes.  If the Ceph cluster is already storing a lot of
   data, be prepared for some fraction of the data to move.
 * The ``ceph-osd`` and ``ceph-mon`` daemons will start requiring the
   feature bits of new connections as soon as they get
   the updated map.  However, already-connected clients are
   effectively grandfathered in, and will misbehave if they do not
   support the new feature.
 * If the CRUSH tunables are set to non-legacy values and then later
   changed back to the default values, ``ceph-osd`` daemons will not be
   required to support the feature.  However, the OSD peering process
   requires examining and understanding old maps.  Therefore, you
   should not run old versions of the ``ceph-osd`` daemon
   if the cluster has previously used non-legacy CRUSH values, even if
   the latest version of the map has been switched back to using the
   legacy defaults.

Tuning CRUSH
------------

The simplest way to adjust CRUSH tunables is by applying them in matched
sets known as *profiles*.  As of the Octopus release these are:

 * ``legacy``: the legacy behavior from argonaut and earlier.
 * ``argonaut``: the legacy values supported by the original argonaut release
 * ``bobtail``: the values supported by the bobtail release
 * ``firefly``: the values supported by the firefly release
 * ``hammer``: the values supported by the hammer release
 * ``jewel``: the values supported by the jewel release
 * ``optimal``: the best (ie optimal) values of the current version of Ceph
 * ``default``: the default values of a new cluster installed from
   scratch. These values, which depend on the current version of Ceph,
   are hardcoded and are generally a mix of optimal and legacy values.
   These values generally match the ``optimal`` profile of the previous
   LTS release, or the most recent release for which we generally expect
   most users to have up-to-date clients for.

You can apply a profile to a running cluster with the command::

 ceph osd crush tunables {PROFILE}

Note that this may result in data movement, potentially quite a bit.  Study
release notes and documentation carefully before changing the profile on a
running cluster, and consider throttling recovery/backfill parameters to
limit the impact of a bolus of backfill.

.. _CRUSH - Controlled, Scalable, Decentralized Placement of Replicated Data: https://ceph.com/assets/pdfs/weil-crush-sc06.pdf


Primary Affinity
================

When a Ceph Client reads or writes data, it first contacts the primary OSD in
each affected PG's acting set. By default, the first OSD in the acting set is
the primary.  For example, in the acting set ``[2, 3, 4]``, ``osd.2`` is
listed first and thus is the primary (aka lead) OSD. Sometimes we know that an
OSD is less well suited to act as the lead than are other OSDs (e.g., it has
a slow drive or a slow controller). To prevent performance bottlenecks
(especially on read operations) while maximizing utilization of your hardware,
you can influence the selection of primary OSDs by adjusting primary affinity
values, or by crafting a CRUSH rule that selects preferred OSDs first.

Tuning primary OSD selection is mainly useful for replicated pools, because
by default read operations are served from the primary OSD for each PG.
For erasure coded (EC) pools, a way to speed up read operations is to enable
**fast read** as described in :ref:`pool-settings`.

A common scenario for primary affinity is when a cluster contains
a mix of drive sizes, for example older racks with 1.9 TB SATA SSDS and newer racks with
3.84TB SATA SSDs.  On average the latter will be assigned double the number of
PGs and thus will serve double the number of write and read operations, thus
they'll be busier than the former.  A rough assignment of primary affinity
inversely proportional to OSD size won't be 100% optimal, but it can readily
achieve a 15% improvement in overall read throughput by utilizing SATA
interface bandwidth and CPU cycles more evenly.

By default, all ceph OSDs have primary affinity of ``1``, which indicates that
any OSD may act as a primary with equal probability.

You can reduce a Ceph OSD's primary affinity so that CRUSH is less likely to choose
the OSD as primary in a PG's acting set.::

	ceph osd primary-affinity <osd-id> <weight>

You may set an OSD's primary affinity to a real number in the range
``[0-1]``, where ``0`` indicates that the OSD may **NOT** be used as a primary
and ``1`` indicates that an OSD may be used as a primary.  When the weight is
between these extremes, it is less likely that
CRUSH will select that OSD as a primary.  The process for
selecting the lead OSD is more nuanced than a simple probability based on
relative affinity values, but measurable results can be achieved even with
first-order approximations of desirable values.

Custom CRUSH Rules
------------------

There are occasional clusters that balance cost and performance by mixing SSDs
and HDDs in the same replicated pool. By setting the primary affinity of HDD
OSDs to ``0`` one can direct operations to the SSD in each acting set. An
alternative is to define a CRUSH rule that always selects an SSD OSD as the
first OSD, then selects HDDs for the remaining OSDs. Thus, each PG's acting
set will contain exactly one SSD OSD as the primary with the balance on HDDs.

For example, the CRUSH rule below::

	rule mixed_replicated_rule {
	        id 11
	        type replicated
	        min_size 1
	        max_size 10
	        step take default class ssd
	        step chooseleaf firstn 1 type host
	        step emit
	        step take default class hdd
	        step chooseleaf firstn 0 type host
	        step emit
	}

chooses an SSD as the first OSD.  Note that for an ``N``-times replicated pool
this rule selects ``N+1`` OSDs to guarantee that ``N`` copies are on different
hosts, because the first SSD OSD might be co-located with any of the ``N`` HDD
OSDs.

This extra storage requirement can be avoided by placing SSDs and HDDs in
different hosts with the tradeoff that hosts with SSDs will receive all client
requests.  You may thus consider faster CPU(s) for SSD hosts and more modest
ones for HDD nodes, since the latter will normally only service recovery
operations.  Here the CRUSH roots ``ssd_hosts`` and ``hdd_hosts`` strictly
must not contain the same servers::

        rule mixed_replicated_rule_two {
               id 1
               type replicated
               min_size 1
               max_size 10
               step take ssd_hosts class ssd
               step chooseleaf firstn 1 type host
               step emit
               step take hdd_hosts class hdd
               step chooseleaf firstn -1 type host
               step emit
        }



Note also that on failure of an SSD, requests to a PG will be served temporarily
from a (slower) HDD OSD until the PG's data has been replicated onto the replacement
primary SSD OSD.

