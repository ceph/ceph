.. index:: control, commands

==================
 Control Commands
==================


Monitor Commands
================

To issue monitor commands, use the ``ceph`` utility:

.. prompt:: bash $

   ceph [-m monhost] {command}

In most cases, monitor commands have the following form:

.. prompt:: bash $

   ceph {subsystem} {command}


System Commands
===============

To display the current cluster status, run the following commands:

.. prompt:: bash $

   ceph -s
   ceph status

To display a running summary of cluster status and major events, run the
following command:

.. prompt:: bash $

   ceph -w

To display the monitor quorum, including which monitors are participating and
which one is the leader, run the following commands:

.. prompt:: bash $

   ceph mon stat
   ceph quorum_status

To query the status of a single monitor, including whether it is in the quorum,
run the following command:

.. prompt:: bash $

   ceph tell mon.[id] mon_status

Here the value of ``[id]`` can be found by consulting the output of ``ceph
-s``.


Authentication Subsystem
========================

To add an OSD keyring for a specific OSD, run the following command:

.. prompt:: bash $

   ceph auth add {osd} {--in-file|-i} {path-to-osd-keyring}

To list the cluster's keys and their capabilities, run the following command:

.. prompt:: bash $

   ceph auth ls


Placement Group Subsystem
=========================

To display the statistics for all placement groups (PGs), run the following
command:

.. prompt:: bash $

   ceph pg dump [--format {format}]

Here the valid formats are ``plain`` (default), ``json`` ``json-pretty``,
``xml``, and ``xml-pretty``.  When implementing monitoring tools and other
tools, it is best to use the ``json`` format.  JSON parsing is more
deterministic than the ``plain`` format (which is more human readable), and the
layout is much more consistent from release to release. The ``jq`` utility is
very useful for extracting data from JSON output.

To display the statistics for all PGs stuck in a specified state, run the
following command:

.. prompt:: bash $

   ceph pg dump_stuck inactive|unclean|stale|undersized|degraded [--format {format}] [-t|--threshold {seconds}]

Here ``--format`` may be ``plain`` (default), ``json``, ``json-pretty``,
``xml``, or ``xml-pretty``.

The ``--threshold`` argument determines the time interval (in seconds) for a PG
to be considered ``stuck`` (default: 300).

PGs might be stuck in any of the following states:

**Inactive** 
    PGs are unable to process reads or writes because they are waiting for an
    OSD that has the most up-to-date data to return to an ``up`` state.

**Unclean** 
    PGs contain objects that have not been replicated the desired number of
    times. These PGs have not yet completed the process of recovering.

**Stale** 
    PGs are in an unknown state, because the OSDs that host them have not
    reported to the monitor cluster for a certain period of time (specified by
    the ``mon_osd_report_timeout`` configuration setting).


To delete a ``lost`` RADOS object or revert an object to its prior state
(either by reverting it to its previous version or by deleting it because it
was just created and has no previous version), run the following command:

.. prompt:: bash $

   ceph pg {pgid} mark_unfound_lost revert|delete


.. _osd-subsystem:

OSD Subsystem
=============

To query OSD subsystem status, run the following command:

.. prompt:: bash $

   ceph osd stat

To write a copy of the most recent OSD map to a file (see :ref:`osdmaptool
<osdmaptool>`), run the following command:

.. prompt:: bash $

   ceph osd getmap -o file

To write a copy of the CRUSH map from the most recent OSD map to a file, run
the following command:

.. prompt:: bash $

   ceph osd getcrushmap -o file

Note that this command is functionally equivalent to the following two
commands:

.. prompt:: bash $

   ceph osd getmap -o /tmp/osdmap
   osdmaptool /tmp/osdmap --export-crush file

To dump the OSD map, run the following command:

.. prompt:: bash $

   ceph osd dump [--format {format}]

The ``--format`` option accepts the following arguments: ``plain`` (default),
``json``, ``json-pretty``, ``xml``, and ``xml-pretty``. As noted above, JSON
format is the recommended format for consumption by tools, scripting, and other
forms of automation. 


To dump the OSD map as a tree that lists one OSD per line and displays
information about the weights and states of the OSDs, run the following
command:

.. prompt:: bash $

   ceph osd tree [--format {format}]

To find out where a specific RADOS object is stored in the system, run a
command of the following form:

.. prompt:: bash $

   ceph osd map <pool-name> <object-name>

To add or move a new OSD (specified by its ID, name, or weight) to a specific
CRUSH location, run the following command:

.. prompt:: bash $

   ceph osd crush set {id} {weight} [{loc1} [{loc2} ...]]

To remove an existing OSD from the CRUSH map, run the following command:

.. prompt:: bash $

   ceph osd crush remove {name}

To remove an existing bucket from the CRUSH map, run the following command:

.. prompt:: bash $

   ceph osd crush remove {bucket-name}

To move an existing bucket from one position in the CRUSH hierarchy to another,
run the following command:

.. prompt:: bash $

   ceph osd crush move {id} {loc1} [{loc2} ...]

To set the CRUSH weight of a specific OSD (specified by ``{name}``) to
``{weight}``, run the following command:

.. prompt:: bash $

   ceph osd crush reweight {name} {weight}

To mark an OSD as ``lost``, run the following command:

.. prompt:: bash $

   ceph osd lost {id} [--yes-i-really-mean-it]

.. warning::
   This could result in permanent data loss. Use with caution!

To create an OSD in the CRUSH map, run the following command:

.. prompt:: bash $

   ceph osd create [{uuid}]

If no UUID is given as part of this command, the UUID will be set automatically
when the OSD starts up.

To remove one or more specific OSDs, run the following command:

.. prompt:: bash $

   ceph osd rm [{id}...]

To display the current ``max_osd`` parameter in the OSD map, run the following
command:

.. prompt:: bash $

   ceph osd getmaxosd

To import a specific CRUSH map, run the following command:

.. prompt:: bash $

   ceph osd setcrushmap -i file

To set the ``max_osd`` parameter in the OSD map, run the following command:

.. prompt:: bash $

   ceph osd setmaxosd

The parameter has a default value of 10000. Most operators will never need to
adjust it.

To mark a specific OSD ``down``, run the following command:

.. prompt:: bash $

   ceph osd down {osd-num}

To mark a specific OSD ``out`` (so that no data will be allocated to it), run
the following command:

.. prompt:: bash $

   ceph osd out {osd-num}

To mark a specific OSD ``in`` (so that data will be allocated to it), run the
following command:

.. prompt:: bash $

   ceph osd in {osd-num}

By using the ``pause`` and ``unpause`` flags in the OSD map, you can pause or
unpause I/O requests. If the flags are set, then no I/O requests will be sent
to any OSD. If the flags are cleared, then pending I/O requests will be resent.
To set or clear these flags, run one of the following commands:

.. prompt:: bash $

   ceph osd pause
   ceph osd unpause

You can assign an override or ``reweight`` weight value to a specific OSD
if the normal CRUSH distribution seems to be suboptimal. The weight of an
OSD helps determine the extent of its I/O requests and data storage: two
OSDs with the same weight will receive approximately the same number of
I/O requests and store approximately the same amount of data. The ``ceph
osd reweight`` command assigns an override weight to an OSD. The weight
value is in the range 0 to 1, and the command forces CRUSH to relocate a
certain amount (1 - ``weight``) of the data that would otherwise be on
this OSD. The command does not change the weights of the buckets above
the OSD in the CRUSH map. Using the command is merely a corrective
measure: for example, if one of your OSDs is at 90% and the others are at
50%, you could reduce the outlier weight to correct this imbalance. To
assign an override weight to a specific OSD, run the following command:

.. prompt:: bash $

   ceph osd reweight {osd-num} {weight}

A cluster's OSDs can be reweighted in order to maintain balance if some OSDs
are being disproportionately utilized. Note that override or ``reweight``
weights have relative values that default to 1.00000. Their values are not
absolute, and these weights must be distinguished from CRUSH weights (which
reflect the absolute capacity of a bucket, as measured in TiB). To reweight
OSDs by utilization, run the following command:

.. prompt:: bash $

   ceph osd reweight-by-utilization [threshold [max_change [max_osds]]] [--no-increasing]

By default, this command adjusts the override weight of OSDs that have ±20%
of the average utilization, but you can specify a different percentage in the
``threshold`` argument. 

To limit the increment by which any OSD's reweight is to be changed, use the
``max_change`` argument (default: 0.05). To limit the number of OSDs that are
to be adjusted, use the ``max_osds`` argument (default: 4). Increasing these
variables can accelerate the reweighting process, but perhaps at the cost of
slower client operations (as a result of the increase in data movement).

You can test the ``osd reweight-by-utilization`` command before running it. To
find out which and how many PGs and OSDs will be affected by a specific use of
the ``osd reweight-by-utilization`` command, run the following command:

.. prompt:: bash $

   ceph osd test-reweight-by-utilization [threshold [max_change max_osds]] [--no-increasing]

The ``--no-increasing`` option can be added to the ``reweight-by-utilization``
and ``test-reweight-by-utilization`` commands in order to prevent any override
weights that are currently less than 1.00000 from being increased. This option
can be useful in certain circumstances: for example, when you are hastily
balancing in order to remedy ``full`` or ``nearfull`` OSDs, or when there are
OSDs being evacuated or slowly brought into service.

Operators of deployments that utilize Nautilus or newer (or later revisions of
Luminous and Mimic) and that have no pre-Luminous clients might likely instead
want to enable the `balancer`` module for ``ceph-mgr``.

.. note:: The ``balancer`` module does the work for you and achieves a more
   uniform result, shuffling less data along the way. When enabling the
   ``balancer`` module, you will want to converge any changed override weights
   back to 1.00000 so that the balancer can do an optimal job. If your cluster
   is very full, reverting these override weights before enabling the balancer
   may cause some OSDs to become full. This means that a phased approach may
   needed.

Add/remove an IP address or CIDR range to/from the blocklist.
When adding to the blocklist,
you can specify how long it should be blocklisted in seconds; otherwise,
it will default to 1 hour. A blocklisted address is prevented from
connecting to any OSD. If you blocklist an IP or range containing an OSD, be aware
that OSD will also be prevented from performing operations on its peers where it
acts as a client. (This includes tiering and copy-from functionality.)

If you want to blocklist a range (in CIDR format), you may do so by
including the ``range`` keyword.

These commands are mostly only useful for failure testing, as
blocklists are normally maintained automatically and shouldn't need
manual intervention. :

.. prompt:: bash $

   ceph osd blocklist ["range"] add ADDRESS[:source_port][/netmask_bits] [TIME]
   ceph osd blocklist ["range"] rm ADDRESS[:source_port][/netmask_bits]

Creates/deletes a snapshot of a pool. :

.. prompt:: bash $

   ceph osd pool mksnap {pool-name} {snap-name}
   ceph osd pool rmsnap {pool-name} {snap-name}

Creates/deletes/renames a storage pool. :

.. prompt:: bash $

   ceph osd pool create {pool-name} [pg_num [pgp_num]]
   ceph osd pool delete {pool-name} [{pool-name} --yes-i-really-really-mean-it]
   ceph osd pool rename {old-name} {new-name}

Changes a pool setting. : 

.. prompt:: bash $

   ceph osd pool set {pool-name} {field} {value}

Valid fields are:

	* ``size``: Sets the number of copies of data in the pool.
	* ``pg_num``: The placement group number.
	* ``pgp_num``: Effective number when calculating pg placement.
	* ``crush_rule``: rule number for mapping placement.

Get the value of a pool setting. :

.. prompt:: bash $

   ceph osd pool get {pool-name} {field}

Valid fields are:

	* ``pg_num``: The placement group number.
	* ``pgp_num``: Effective number of placement groups when calculating placement.


Sends a scrub command to OSD ``{osd-num}``. To send the command to all OSDs, use ``*``. :

.. prompt:: bash $

   ceph osd scrub {osd-num}

Sends a repair command to OSD.N. To send the command to all OSDs, use ``*``. :

.. prompt:: bash $

   ceph osd repair N

Runs a simple throughput benchmark against OSD.N, writing ``TOTAL_DATA_BYTES``
in write requests of ``BYTES_PER_WRITE`` each. By default, the test
writes 1 GB in total in 4-MB increments.
The benchmark is non-destructive and will not overwrite existing live
OSD data, but might temporarily affect the performance of clients
concurrently accessing the OSD. :

.. prompt:: bash $

   ceph tell osd.N bench [TOTAL_DATA_BYTES] [BYTES_PER_WRITE]

To clear an OSD's caches between benchmark runs, use the 'cache drop' command :

.. prompt:: bash $

   ceph tell osd.N cache drop

To get the cache statistics of an OSD, use the 'cache status' command :

.. prompt:: bash $

   ceph tell osd.N cache status

MDS Subsystem
=============

Change configuration parameters on a running mds. :

.. prompt:: bash $

   ceph tell mds.{mds-id} config set {setting} {value}

Example:

.. prompt:: bash $

   ceph tell mds.0 config set debug_ms 1

Enables debug messages. :

.. prompt:: bash $

   ceph mds stat

Displays the status of all metadata servers. :

.. prompt:: bash $

   ceph mds fail 0

Marks the active MDS as failed, triggering failover to a standby if present.

.. todo:: ``ceph mds`` subcommands missing docs: set, dump, getmap, stop, setmap


Mon Subsystem
=============

Show monitor stats:

.. prompt:: bash $

   ceph mon stat

::

	e2: 3 mons at {a=127.0.0.1:40000/0,b=127.0.0.1:40001/0,c=127.0.0.1:40002/0}, election epoch 6, quorum 0,1,2 a,b,c


The ``quorum`` list at the end lists monitor nodes that are part of the current quorum.

This is also available more directly:

.. prompt:: bash $

   ceph quorum_status -f json-pretty
	
.. code-block:: javascript	

	{
	    "election_epoch": 6,
	    "quorum": [
		0,
		1,
		2
	    ],
	    "quorum_names": [
		"a",
		"b",
		"c"
	    ],
	    "quorum_leader_name": "a",
	    "monmap": {
		"epoch": 2,
		"fsid": "ba807e74-b64f-4b72-b43f-597dfe60ddbc",
		"modified": "2016-12-26 14:42:09.288066",
		"created": "2016-12-26 14:42:03.573585",
		"features": {
		    "persistent": [
			"kraken"
		    ],
		    "optional": []
		},
		"mons": [
		    {
			"rank": 0,
			"name": "a",
			"addr": "127.0.0.1:40000\/0",
			"public_addr": "127.0.0.1:40000\/0"
		    },
		    {
			"rank": 1,
			"name": "b",
			"addr": "127.0.0.1:40001\/0",
			"public_addr": "127.0.0.1:40001\/0"
		    },
		    {
			"rank": 2,
			"name": "c",
			"addr": "127.0.0.1:40002\/0",
			"public_addr": "127.0.0.1:40002\/0"
		    }
		]
	    }
	}
	  

The above will block until a quorum is reached.

For a status of just a single monitor:

.. prompt:: bash $

   ceph tell mon.[name] mon_status
	
where the value of ``[name]`` can be taken from ``ceph quorum_status``. Sample
output::
	
	{
	    "name": "b",
	    "rank": 1,
	    "state": "peon",
	    "election_epoch": 6,
	    "quorum": [
		0,
		1,
		2
	    ],
	    "features": {
		"required_con": "9025616074522624",
		"required_mon": [
		    "kraken"
		],
		"quorum_con": "1152921504336314367",
		"quorum_mon": [
		    "kraken"
		]
	    },
	    "outside_quorum": [],
	    "extra_probe_peers": [],
	    "sync_provider": [],
	    "monmap": {
		"epoch": 2,
		"fsid": "ba807e74-b64f-4b72-b43f-597dfe60ddbc",
		"modified": "2016-12-26 14:42:09.288066",
		"created": "2016-12-26 14:42:03.573585",
		"features": {
		    "persistent": [
			"kraken"
		    ],
		    "optional": []
		},
		"mons": [
		    {
			"rank": 0,
			"name": "a",
			"addr": "127.0.0.1:40000\/0",
			"public_addr": "127.0.0.1:40000\/0"
		    },
		    {
			"rank": 1,
			"name": "b",
			"addr": "127.0.0.1:40001\/0",
			"public_addr": "127.0.0.1:40001\/0"
		    },
		    {
			"rank": 2,
			"name": "c",
			"addr": "127.0.0.1:40002\/0",
			"public_addr": "127.0.0.1:40002\/0"
		    }
		]
	    }
	}

A dump of the monitor state:

.. prompt:: bash $

   ceph mon dump

::

	dumped monmap epoch 2
	epoch 2
	fsid ba807e74-b64f-4b72-b43f-597dfe60ddbc
	last_changed 2016-12-26 14:42:09.288066
	created 2016-12-26 14:42:03.573585
	0: 127.0.0.1:40000/0 mon.a
	1: 127.0.0.1:40001/0 mon.b
	2: 127.0.0.1:40002/0 mon.c

