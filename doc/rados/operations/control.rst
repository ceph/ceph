.. index:: control, commands

==================
 Control Commands
==================


Monitor Commands
================

Monitor commands are issued using the ``ceph`` utility::

	ceph [-m monhost] {command}

The command is usually (though not always) of the form::

	ceph {subsystem} {command}


System Commands
===============

Execute the following to display the current cluster status.  ::

	ceph -s
	ceph status

Execute the following to display a running summary of cluster status
and major events. ::

	ceph -w

Execute the following to show the monitor quorum, including which monitors are
participating and which one is the leader. ::

	ceph mon stat
	ceph quorum_status

Execute the following to query the status of a single monitor, including whether
or not it is in the quorum. ::

	ceph tell mon.[id] mon_status

where the value of ``[id]`` can be determined, e.g., from ``ceph -s``.


Authentication Subsystem
========================

To add a keyring for an OSD, execute the following::

	ceph auth add {osd} {--in-file|-i} {path-to-osd-keyring}

To list the cluster's keys and their capabilities, execute the following::

	ceph auth ls


Placement Group Subsystem
=========================

To display the statistics for all placement groups (PGs), execute the following:: 

	ceph pg dump [--format {format}]

The valid formats are ``plain`` (default), ``json`` ``json-pretty``, ``xml``, and ``xml-pretty``.
When implementing monitoring and other tools, it is best to use ``json`` format.
JSON parsing is more deterministic than the human-oriented ``plain``, and the layout is much
less variable from release to release.  The ``jq`` utility can be invaluable when extracting
data from JSON output.

To display the statistics for all placement groups stuck in a specified state, 
execute the following:: 

	ceph pg dump_stuck inactive|unclean|stale|undersized|degraded [--format {format}] [-t|--threshold {seconds}]


``--format`` may be ``plain`` (default), ``json``, ``json-pretty``, ``xml``, or ``xml-pretty``.

``--threshold`` defines how many seconds "stuck" is (default: 300)

**Inactive** Placement groups cannot process reads or writes because they are waiting for an OSD
with the most up-to-date data to come back.

**Unclean** Placement groups contain objects that are not replicated the desired number
of times. They should be recovering.

**Stale** Placement groups are in an unknown state - the OSDs that host them have not
reported to the monitor cluster in a while (configured by
``mon_osd_report_timeout``).

Delete "lost" objects or revert them to their prior state, either a previous version
or delete them if they were just created. ::

	ceph pg {pgid} mark_unfound_lost revert|delete


.. _osd-subsystem:

OSD Subsystem
=============

Query OSD subsystem status. ::

	ceph osd stat

Write a copy of the most recent OSD map to a file. See
:ref:`osdmaptool <osdmaptool>`. ::

	ceph osd getmap -o file

Write a copy of the crush map from the most recent OSD map to
file. ::

	ceph osd getcrushmap -o file

The foregoing is functionally equivalent to ::

	ceph osd getmap -o /tmp/osdmap
	osdmaptool /tmp/osdmap --export-crush file

Dump the OSD map. Valid formats for ``-f`` are ``plain``, ``json``, ``json-pretty``,
``xml``, and ``xml-pretty``. If no ``--format`` option is given, the OSD map is 
dumped as plain text.  As above, JSON format is best for tools, scripting, and other automation. ::

	ceph osd dump [--format {format}]

Dump the OSD map as a tree with one line per OSD containing weight
and state. ::

	ceph osd tree [--format {format}]

Find out where a specific object is or would be stored in the system::

	ceph osd map <pool-name> <object-name>

Add or move a new item (OSD) with the given id/name/weight at the specified
location. ::

	ceph osd crush set {id} {weight} [{loc1} [{loc2} ...]]

Remove an existing item (OSD) from the CRUSH map. ::

	ceph osd crush remove {name}

Remove an existing bucket from the CRUSH map. ::

	ceph osd crush remove {bucket-name}

Move an existing bucket from one position in the hierarchy to another.  ::

	ceph osd crush move {id} {loc1} [{loc2} ...]

Set the weight of the item given by ``{name}`` to ``{weight}``. ::

	ceph osd crush reweight {name} {weight}

Mark an OSD as ``lost``. This may result in permanent data loss. Use with caution. ::

	ceph osd lost {id} [--yes-i-really-mean-it]

Create a new OSD. If no UUID is given, it will be set automatically when the OSD
starts up. ::

	ceph osd create [{uuid}]

Remove the given OSD(s). ::

	ceph osd rm [{id}...]

Query the current ``max_osd`` parameter in the OSD map. ::

	ceph osd getmaxosd

Import the given crush map. ::

	ceph osd setcrushmap -i file

Set the ``max_osd`` parameter in the OSD map. This defaults to 10000 now so
most admins will never need to adjust this. ::

	ceph osd setmaxosd

Mark OSD ``{osd-num}`` down. ::

	ceph osd down {osd-num}

Mark OSD ``{osd-num}`` out of the distribution (i.e. allocated no data). ::

	ceph osd out {osd-num}

Mark ``{osd-num}`` in the distribution (i.e. allocated data). ::

	ceph osd in {osd-num}

Set or clear the pause flags in the OSD map. If set, no IO requests
will be sent to any OSD. Clearing the flags via unpause results in
resending pending requests. ::

	ceph osd pause
	ceph osd unpause

Set the override weight (reweight) of ``{osd-num}`` to ``{weight}``. Two OSDs with the
same weight will receive roughly the same number of I/O requests and
store approximately the same amount of data. ``ceph osd reweight``
sets an override weight on the OSD. This value is in the range 0 to 1,
and forces CRUSH to re-place (1-weight) of the data that would
otherwise live on this drive. It does not change weights assigned
to the buckets above the OSD in the crush map, and is a corrective
measure in case the normal CRUSH distribution is not working out quite
right. For instance, if one of your OSDs is at 90% and the others are
at 50%, you could reduce this weight to compensate. ::

	ceph osd reweight {osd-num} {weight}

Balance OSD fullness by reducing the override weight of OSDs which are
overly utilized.  Note that these override aka ``reweight`` values
default to 1.00000 and are relative only to each other; they not absolute.
It is crucial to distinguish them from CRUSH weights, which reflect the
absolute capacity of a bucket in TiB.  By default this command adjusts
override weight on OSDs which have + or - 20% of the average utilization,
but if you include a ``threshold`` that percentage will be used instead. ::

	ceph osd reweight-by-utilization [threshold [max_change [max_osds]]] [--no-increasing]

To limit the step by which any OSD's reweight will be changed, specify
``max_change`` which defaults to 0.05.  To limit the number of OSDs that will
be adjusted, specify ``max_osds`` as well; the default is 4.  Increasing these
parameters can speed leveling of OSD utilization, at the potential cost of
greater impact on client operations due to more data moving at once.

To determine which and how many PGs and OSDs will be affected by a given invocation
you can test before executing. ::

	ceph osd test-reweight-by-utilization [threshold [max_change max_osds]] [--no-increasing]

Adding ``--no-increasing`` to either command prevents increasing any
override weights that are currently < 1.00000.  This can be useful when
you are balancing in a hurry to remedy ``full`` or ``nearful`` OSDs or
when some OSDs are being evacuated or slowly brought into service.

Deployments utilizing Nautilus (or later revisions of Luminous and Mimic)
that have no pre-Luminous cients may instead wish to instead enable the
`balancer`` module for ``ceph-mgr``.

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
manual intervention. ::

	ceph osd blocklist ["range"] add ADDRESS[:source_port][/netmask_bits] [TIME]
	ceph osd blocklist ["range"] rm ADDRESS[:source_port][/netmask_bits]

Creates/deletes a snapshot of a pool. ::

	ceph osd pool mksnap {pool-name} {snap-name}
	ceph osd pool rmsnap {pool-name} {snap-name}

Creates/deletes/renames a storage pool. ::

	ceph osd pool create {pool-name} [pg_num [pgp_num]]
	ceph osd pool delete {pool-name} [{pool-name} --yes-i-really-really-mean-it]
	ceph osd pool rename {old-name} {new-name}

Changes a pool setting. :: 

	ceph osd pool set {pool-name} {field} {value}

Valid fields are:

	* ``size``: Sets the number of copies of data in the pool.
	* ``pg_num``: The placement group number.
	* ``pgp_num``: Effective number when calculating pg placement.
	* ``crush_rule``: rule number for mapping placement.

Get the value of a pool setting. ::

	ceph osd pool get {pool-name} {field}

Valid fields are:

	* ``pg_num``: The placement group number.
	* ``pgp_num``: Effective number of placement groups when calculating placement.


Sends a scrub command to OSD ``{osd-num}``. To send the command to all OSDs, use ``*``. ::

	ceph osd scrub {osd-num}

Sends a repair command to OSD.N. To send the command to all OSDs, use ``*``. ::

	ceph osd repair N

Runs a simple throughput benchmark against OSD.N, writing ``TOTAL_DATA_BYTES``
in write requests of ``BYTES_PER_WRITE`` each. By default, the test
writes 1 GB in total in 4-MB increments.
The benchmark is non-destructive and will not overwrite existing live
OSD data, but might temporarily affect the performance of clients
concurrently accessing the OSD. ::

	ceph tell osd.N bench [TOTAL_DATA_BYTES] [BYTES_PER_WRITE]

To clear an OSD's caches between benchmark runs, use the 'cache drop' command ::

	ceph tell osd.N cache drop

To get the cache statistics of an OSD, use the 'cache status' command ::

	ceph tell osd.N cache status

MDS Subsystem
=============

Change configuration parameters on a running mds. ::

	ceph tell mds.{mds-id} config set {setting} {value}

Example::

	ceph tell mds.0 config set debug_ms 1

Enables debug messages. ::

	ceph mds stat

Displays the status of all metadata servers. ::

	ceph mds fail 0

Marks the active MDS as failed, triggering failover to a standby if present.

.. todo:: ``ceph mds`` subcommands missing docs: set, dump, getmap, stop, setmap


Mon Subsystem
=============

Show monitor stats::

	ceph mon stat

	e2: 3 mons at {a=127.0.0.1:40000/0,b=127.0.0.1:40001/0,c=127.0.0.1:40002/0}, election epoch 6, quorum 0,1,2 a,b,c


The ``quorum`` list at the end lists monitor nodes that are part of the current quorum.

This is also available more directly::

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

For a status of just a single monitor::

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

A dump of the monitor state::

	ceph mon dump

	dumped monmap epoch 2
	epoch 2
	fsid ba807e74-b64f-4b72-b43f-597dfe60ddbc
	last_changed 2016-12-26 14:42:09.288066
	created 2016-12-26 14:42:03.573585
	0: 127.0.0.1:40000/0 mon.a
	1: 127.0.0.1:40001/0 mon.b
	2: 127.0.0.1:40002/0 mon.c

