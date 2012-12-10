============
 CRUSH Maps
============

The :abbr:`CRUSH (Controlled Replication Under Scalable Hashing)` algorithm
determines how to store and retrieve data by computing data storage locations.
CRUSH empowers Ceph clients to communicate with OSDs directly rather than
through a centralized server or broker. With an algorithmically determined
method of storing and retrieving data, Ceph avoids a single point of failure, a
performance bottleneck, and a physical limit to its scalability.

CRUSH requires a map of your cluster, and uses the CRUSH map to pseudo-randomly 
store and retrieve data in OSDs with a uniform distribution of data across the 
cluster. For a detailed discussion of CRUSH, see 
`CRUSH - Controlled, Scalable, Decentralized Placement of Replicated Data`_

.. _CRUSH - Controlled, Scalable, Decentralized Placement of Replicated Data: http://ceph.com/papers/weil-crush-sc06.pdf

CRUSH Maps contain a list of :abbr:`OSDs (Object Storage Devices)`, a list of
'buckets' for aggregating the devices into physical locations, and a list of
rules that tell CRUSH how it should replicate data in a Ceph cluster's pools. By
reﬂecting the  underlying physical organization of the installation, CRUSH can
model—and thereby  address—potential sources of correlated device failures.
Typical sources include  physical proximity, a shared power source, and a shared
network. By encoding this  information into the cluster map, CRUSH placement
policies can  separate object replicas across different failure domains while
still maintaining  the desired distribution. For example, to address the
possibility of concurrent failures, it may be desirable to ensure that data
replicas are on devices in  different shelves, racks, power supplies,
controllers, and/or physical locations.

When you create a configuration file and deploy Ceph with ``mkcephfs``, Ceph
generates a default CRUSH map for your configuration. The default CRUSH map is
fine for your Ceph sandbox environment. However, when you deploy a large-scale
data cluster, you should give significant consideration to developing a custom
CRUSH map, because  it will help you manage your Ceph cluster, improve
performance and ensure data safety. 

For example, if an OSD goes down, a CRUSH Map can help you can locate
the physical data center, room, row and rack of the host with the failed OSD in
the event you need to use onsite support or replace hardware. 

Similarly, CRUSH may help you identify faults more quickly. For example, if all
OSDs in a particular rack go down simultaneously, the fault may lie with a
network switch or power to the rack or the network switch rather than the 
OSDs themselves.

A custom CRUSH map can also help you identify the physical locations where
Ceph stores redundant copies of data when the placement group(s) associated
with a failed host are in a degraded state.

`Inktank`_ provides excellent premium support for developing CRUSH maps.

.. _Inktank: http://www.inktank.com

.. note:: Lines of code in example boxes may extend past the edge of the box. 
   Please scroll when reading or copying longer examples.

Editing a CRUSH Map
===================

To edit an existing CRUSH map:

#. `Get the CRUSH Map`_.
#. `Decompile`_ the CRUSH Map.
#. Edit at least one of `Devices`_, `Buckets`_ and `Rules`_.
#. `Recompile`_ the CRUSH Map.
#. `Set the CRUSH Map`_.

To activate CRUSH Map rules for a specific pool, identify the common ruleset
number for those rules and specify that ruleset number for the pool. See `Set
Pool Values`_ for details. 

.. _Get the CRUSH Map: #getcrushmap
.. _Decompile: #decompilecrushmap
.. _Devices: #crushmapdevices
.. _Buckets: #crushmapbuckets
.. _Rules: #crushmaprules
.. _Recompile: #compilecrushmap
.. _Set the CRUSH Map: #setcrushmap
.. _Set Pool Values: ../pools#setpoolvalues

.. _getcrushmap:

Get a CRUSH Map
---------------

To get the CRUSH Map for your cluster, execute the following:: 

	ceph osd getcrushmap -o {compiled-crushmap-filename}

Ceph will output (-o) a compiled CRUSH Map to the filename you specified. Since
the CRUSH Map is in a compiled form, you must decompile it first before you can
edit it. 

.. _decompilecrushmap:

Decompile a CRUSH Map
---------------------

To decompile a CRUSH Map, execute the following:: 

	crushtool -d {compiled-crushmap-filename} -o {decompiled-crushmap-filename}

Ceph will decompile (-d) the compiled CRUSH map and output (-o) it to the 
filename you specified.


.. _compilecrushmap:

Compile a CRUSH Map
-------------------

To compile a CRUSH Map, execute the following:: 

	crushtool -c {decompiled-crush-map-filename} -o {compiled-crush-map-filename}

Ceph will store a compiled CRUSH map to the filename you specified. 


.. _setcrushmap:

Set a CRUSH Map
---------------

To set the CRUSH Map for your cluster, execute the following:: 

	ceph osd setcrushmap -i  {compiled-crushmap-filename}

Ceph will input the compiled CRUSH Map of the filename you specified as the
CRUSH Map for the cluster.



CRUSH Map Parameters
====================

There are three main sections to a CRUSH Map. 

#. Devices consist of any object storage device--i.e., the hard disk 
   corresponding to a ``ceph-osd`` daemon.
#. Buckets consist of a hierarchical aggregation of storage locations 
   (e.g., rows, racks, hosts, etc.) and their assigned weights.
#. Rules consist of the manner of selecting buckets 


.. _crushmapdevices:

CRUSH Map Devices
-----------------

To map placement groups to OSDs, a CRUSH Map requires a list of OSD devices 
(i.e., the name of the OSD daemon). The list of devices appears first in the 
CRUSH Map. ::

	#devices
	device {num} {osd.name}

For example:: 

	#devices
	device 0 osd.0
	device 1 osd.1
	device 2 osd.2
	device 3 osd.3
	
As a general rule, an OSD daemon maps to a single disk or to a RAID. 


.. _crushmapbuckets:

CRUSH Map Buckets
-----------------

CRUSH maps support the notion of 'buckets', which may be thought of as nodes
that aggregate other buckets into a hierarchy of physical locations, where OSD
devices are the leaves of the hierarchy. The following table lists the default
types. 

+------+----------+-------------------------------------------------------+
| Type | Location    | Description                                        |
+======+=============+====================================================+
|  0   |   OSD       | An OSD daemon (e.g., osd.1, osd.2, etc).           |
+------+-------------+----------------------------------------------------+
|  1   |   Host      | A host name containing one or more OSDs.           |
+------+-------------+----------------------------------------------------+
|  2   |   Rack      | A computer rack. The default is ``unknownrack``.   |
+------+-------------+----------------------------------------------------+
|  3   |   Row       | A row in a series of racks.                        |
+------+-------------+----------------------------------------------------+
|  4   |   Room      | A room containing racks and rows of hosts.         |
+------+-------------+----------------------------------------------------+
|  5   | Data Center | A physical data center containing rooms.           |
+------+-------------+----------------------------------------------------+
|  6   |   Pool      | A data storage pool for storing objects.           |
+------+-------------+----------------------------------------------------+

.. tip:: You can remove these types and create your own bucket types.

Ceph's deployment tools generate a CRUSH map that contains a bucket for each
host, and a pool named "default," which is useful for the default ``data``,
``metadata`` and ``rbd`` pools. The remaining bucket types provide a means for
storing information about the physical location of nodes/buckets, which makes
cluster  administration much easier when OSDs, hosts, or network hardware
malfunction and the administrator needs access to physical hardware.

.. tip: The term "bucket" used in the context of CRUSH means a Ceph pool, a 
   location, or a piece of physical hardware. It is a different concept from 
   the term "bucket" when used in the context of RADOS Gateway APIs.

A bucket has a type, a unique name (string), a unique ID expressed as a negative
integer, a weight relative to the total capacity/capability of its item(s), the 
bucket algorithm (``straw`` by default), and the hash (``0`` by default, reflecting
CRUSH Hash ``rjenkins1``). A bucket may have one or more items. The items may 
consist of other buckets or OSDs. Items may have a weight that reflects the
relative weight of the item. 

:: 

	[bucket-type] [bucket-name] {
		id [a unique negative numeric ID]
		weight [the relative capacity/capability of the item(s)]
		alg [the bucket type: uniform | list | tree | straw ]
		hash [the hash type: 0 by default]
		item [item-name] weight [weight]	
	}

The following example illustrates how you can use buckets to aggregate a pool and 
physical locations like a datacenter, a room, a rack and a row. :: 

	host ceph-osd-server-1 {
		id -17
		alg straw
		hash 0
		item osd.0 weight 1.00
		item osd.1 weight 1.00
	}

	row rack-1-row-1 {
		id -16
		alg straw
		hash 0
		item ceph-osd-server-1 2.00
	}

	rack rack-3 {
		id -15
		alg straw 
		hash 0
		item rack-3-row-1 weight 2.00
		item rack-3-row-2 weight 2.00
		item rack-3-row-3 weight 2.00
		item rack-3-row-4 weight 2.00
		item rack-3-row-5 weight 2.00
	}
	
	rack rack-2 {
		id -14
		alg straw
		hash 0
		item rack-2-row-1 weight 2.00
		item rack-2-row-2 weight 2.00
		item rack-2-row-3 weight 2.00
		item rack-2-row-4 weight 2.00
		item rack-2-row-5 weight 2.00
	}
	
	rack rack-1 {
		id -13
		alg straw
		hash 0
		item rack-1-row-1 weight 2.00
		item rack-1-row-2 weight 2.00
		item rack-1-row-3 weight 2.00
		item rack-1-row-4 weight 2.00
		item rack-1-row-5 weight 2.00
	}
	
	room server-room-1 {
		id -12
		alg straw
		hash 0
		item rack-1 weight 10.00
		item rack-2 weight 10.00
		item rack-3 weight 10.00
	}
	
	datacenter dc-1 {
		id -11
		alg straw
		hash 0
		item server-room-1 weight 30.00
		item server-room-2 weight 30.00	
	}
	
	pool data {
		id -10		
		alg straw
		hash 0
		item dc-1 weight 60.00
		item dc-2 weight 60.00
	}

.. _crushmaprules:

CRUSH Map Rules
---------------

CRUSH maps support the notion of 'CRUSH rules', which are the rules that
determine data placement for a pool. For large clusters, you will likely create
many pools where each pool may have its own CRUSH ruleset and rules. The default
CRUSH map has a rule for each pool, and one ruleset assigned to each of the
default pools, which include:

- ``data``
- ``metadata``
- ``rbd``

.. note:: In most cases, you will not need to modify the default rules. When
   you create a new pool, its default ruleset is ``0``.

A rule takes the following form:: 

	rule [rulename] {
	
		ruleset [ruleset]
		type [type]
		min_size [min-size]
		max_size [max-size]
		step [step]
		
	}


``ruleset``

:Description: A means of classifying a rule as belonging to a set of rules. Activated by `setting the ruleset in a pool`_. 
:Purpose: A component of the rule mask.
:Type: Integer
:Required: Yes
:Default: 0

.. _setting the ruleset in a pool: ../pools#setpoolvalues


``type``

:Description: Describes a rule for either a hard disk (replicated) or a RAID.
:Purpose: A component of the rule mask. 
:Type: String
:Required: Yes
:Default: ``replicated``
:Valid Values: Currently only ``replicated``

``min_size``

:Description: If a placement group makes fewer replicas than this number, CRUSH will NOT select this rule.
:Type: Integer
:Purpose: A component of the rule mask.
:Required: Yes
:Default: ``1``

``max_size``

:Description: If a placement group makes more replicas than this number, CRUSH will NOT select this rule.
:Type: Integer
:Purpose: A component of the rule mask.
:Required: Yes
:Default: 10


``step take {bucket}``

:Description: Takes a bucket name, and begins iterating down the tree.
:Purpose: A component of the rule.
:Required: Yes
:Example: ``step take data``


``step choose firstn {num} type {bucket-type}``

:Description: Selects the number of buckets of the given type. Where ``N`` is the number of options available, if ``{num} > 0 && < N``, choose that many buckets; if ``{num} < 0``, it means ``N - {num}``; and, if ``{num} == 0``, choose ``N`` buckets (all available).
:Purpose: A component of the rule.
:Prerequisite: Follows ``step take`` or ``step choose``.  
:Example: ``step choose firstn 1 type row``  


``step emit`` 

:Description: Outputs the current value and empties the stack. Typically used at the end of a rule, but may also be used to from different trees in the same rule.
:Purpose: A component of the rule.
:Prerequisite: Follows ``step choose``.
:Example: ``step emit``

.. important:: To activate one or more rules with a common ruleset number to a pool, set the ruleset number to the pool.

Placing Different Pools on Different OSDS:
==========================================

It's possible to have multiple independent crush heirarchies within the same
crush map.  Suppose you want to have pools default to osds backed by large
spinning disks but have some pools mapped to osds backed by fast SSDs::

  device 0 osd.0
  device 1 osd.1
  device 2 osd.2
  device 3 osd.3
  device 4 osd.4
  device 5 osd.5
  device 6 osd.6
  device 7 osd.7

	host ceph-osd-ssd-server-1 {
		id -1
		alg straw
		hash 0
		item osd.0 weight 1.00
		item osd.1 weight 1.00
	}

	host ceph-osd-ssd-server-2 {
		id -2
		alg straw
		hash 0
		item osd.2 weight 1.00
		item osd.3 weight 1.00
	}

	host ceph-osd-platter-server-1 {
		id -3
		alg straw
		hash 0
		item osd.4 weight 1.00
		item osd.5 weight 1.00
	}

	host ceph-osd-platter-server-2 {
		id -4
		alg straw
		hash 0
		item osd.6 weight 1.00
		item osd.7 weight 1.00
	}

	root platter {
		id -5	
		alg straw
		hash 0
		item ceph-osd-platter-server-1 weight 2.00
		item ceph-osd-platter-server-2 weight 2.00
	}

	root ssd {
		id -6	
		alg straw
		hash 0
		item ceph-osd-ssd-server-1 weight 2.00
		item ceph-osd-ssd-server-2 weight 2.00
	}

	rule data {
		ruleset 0
		type replicated
		min_size 2
		max_size 2
		step take platter
		step chooseleaf 0 type host
		step emit
	}

	rule metadata {
		ruleset 1
		type replicated
		min_size 0
		max_size 10
		step take platter
		step chooseleaf 0 type host
		step emit
	}

	rule rbd {
		ruleset 2
		type replicated
		min_size 0
		max_size 10
		step take platter
		step chooseleaf 0 type host
		step emit
	}

	rule platter {
		ruleset 3
		type replicated
		min_size 0
		max_size 10
		step take platter
		step chooseleaf 0 type host
		step emit
	}

	rule ssd {
		ruleset 4
		type replicated
		min_size 0
		max_size 10
		step take ssd
		step chooseleaf 0 type host
		step emit
	}

	rule ssd-primary {
		ruleset 4
		type replicated
		min_size 0
		max_size 10
		step take ssd
		step chooseleaf 1 type host
		step emit
		step take platter
		step chooseleaf -1 type host
		step emit
	}

You can then set a pool to use the ssd rule by::

  ceph osd pool set <poolname> crush_ruleset 4

Similarly, using the ssd-primary rule will cause
each pg in the pool to be placed with an SSD as
the primary and platters as the replicas.

.. _addosd:

Add/Move an OSD
===============

To add or move an OSD in the CRUSH map of a running cluster, execute the
following::

	ceph osd crush set {id} {name} {weight} pool={pool-name}  [{bucket-type}={bucket-name} ...]

Where:

``id``

:Description: The numeric ID of the OSD.
:Type: Integer
:Required: Yes
:Example: ``0``


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


``pool``

:Description:  By default, the CRUSH hierarchy contains the pool default as its root. 
:Type: Key/value pair.
:Required: Yes
:Example: ``pool=default``


``bucket-type``

:Description: You may specify the OSD's location in the CRUSH hierarchy. 
:Type: Key/value pairs.
:Required: No
:Example: ``datacenter=dc1 room=room1 row=foo rack=bar host=foo-bar-1``


The following example adds ``osd.0`` to the hierarchy, or moves the OSD from a
previous location. :: 

	ceph osd crush set 0 osd.0 1.0 pool=data datacenter=dc1 room=room1 row=foo rack=bar host=foo-bar-1


Adjust an OSD's CRUSH Weight
============================

To adjust an OSD's crush weight in the CRUSH map of a running cluster, execute
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
=============

To remove an OSD from the CRUSH map of a running cluster, execute the following::

	ceph osd crush remove {name}  

Where:

``name``

:Description: The full name of the OSD. 
:Type: String
:Required: Yes
:Example: ``osd.0``


Move a Bucket
=============

To move a bucket to a different location or position in the CRUSH map hierarchy,
execute the following:: 

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


Tunables
========

.. versionadded:: 0.48

There are several magic numbers that were used in the original CRUSH
implementation that have proven to be poor choices.  To support
the transition away from them, newer versions of CRUSH (starting with
the v0.48 argonaut series) allow the values to be adjusted or tuned.

Clusters running recent Ceph releases support using the tunable values
in the CRUSH maps.  However, older clients and daemons will not correctly interact
with clusters using the "tuned" CRUSH maps.  To detect this situation,
there is now a feature bit ``CRUSH_TUNABLES`` (value 0x40000) to
reflect support for tunables.

If the OSDMap currently used by the ``ceph-mon`` or ``ceph-osd``
daemon has non-legacy values, it will require the ``CRUSH_TUNABLES``
feature bit from clients and daemons who connect to it.  This means
that old clients will not be able to connect.

At some future point in time, newly created clusters will have
improved default values for the tunables.  This is a matter of waiting
until the support has been present in the Linux kernel clients long
enough to make this a painless transition for most users.

Impact of Legacy Values
-----------------------

The legacy values result in several misbehaviors:

 * For hiearchies with a small number of devices in the leaf buckets,
   some PGs map to fewer than the desired number of replicas.  This
   commonly happens for hiearchies with "host" nodes with a small
   number (1-3) of OSDs nested beneath each one.

 * For large clusters, some small percentages of PGs map to less than
   the desired number of OSDs.  This is more prevalent when there are
   several layers of the hierarchy (e.g., row, rack, host, osd).

 * When some OSDs are marked out, the data tends to get redistributed
   to nearby OSDs instead of across the entire hierarchy.

Which client versions support tunables
--------------------------------------

 * argonaut series, v0.48.1 or later
 * v0.49 or later
 * Linux kernel version v3.5 or later (for the file system and RBD kernel clients)

A few important points
----------------------

 * Adjusting these values will result in the shift of some PGs between
   storage nodes.  If the Ceph cluster is already storing a lot of
   data, be prepared for some fraction of the data to move.
 * The ``ceph-osd`` and ``ceph-mon`` daemons will start requiring the
   ``CRUSH_TUNABLES`` feature of new connections as soon as they get
   the updated map.  However, already-connected clients are
   effectively grandfathered in, and will misbehave if they do not
   support the new feature.
 * If the CRUSH tunables are set to non-legacy values and then later
   changed back to the defult values, ``ceph-osd`` daemons will not be
   required to support the feature.  However, the OSD peering process
   requires examining and understanding old maps.  Therefore, you
   should not run old (pre-v0.48) versions of the ``ceph-osd`` daemon
   if the cluster has previosly used non-legacy CRUSH values, even if
   the latest version of the map has been switched back to using the
   legacy defaults.

Tuning CRUSH
------------

If you can ensure that all clients are running recent code, you can
adjust the tunables by extracting the CRUSH map, modifying the values,
and reinjecting it into the cluster.

* Extract the latest CRUSH map::

	ceph osd getcrushmap -o /tmp/crush

* Adjust tunables.  These values appear to offer the best behavior
  for both large and small clusters we tested with.  You will need to
  additionally specify the ``--enable-unsafe-tunables`` argument to
  ``crushtool`` for this to work.  Please use this option with
  extreme care.::

	crushtool -i /tmp/crush --set-choose-local-tries 0 --set-choose-local-fallback-tries 0 --set-choose-total-tries 50 -o /tmp/crush.new

* Reinject modified map::

	ceph osd setcrushmap -i /tmp/crush.new

Legacy values
-------------

For reference, the legacy values for the CRUSH tunables can be set
with::

   crushtool -i /tmp/crush --set-choose-local-tries 2 --set-choose-local-fallback-tries 5 --set-choose-total-tries 19 -o /tmp/crush.legacy

Again, the special ``--enable-unsafe-tunables`` option is required.
Further, as noted above, be careful running old versions of the
``ceph-osd`` daemon after reverting to legacy values as the feature
bit is not perfectly enforced.

