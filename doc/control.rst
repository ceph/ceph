.. index:: control, commands

=================
 Control commands
=================

Monitor commands
----------------

Monitor commands are issued using the ceph utility::

	$ ceph [-m monhost] command

where the command is usually (though not always) of the form::

	$ ceph subsystem command

System commands
---------------

::

	$ ceph -s
        $ ceph status

Shows an overview of the current status of the cluster.  ::

	$ ceph -w

Shows a running summary of the status of the cluster, and major events.

	$ ceph quorum_status

Show the monitor quorum, including which monitors are participating and which one
is the leader.

	$ ceph [-m monhost] mon_status

Query the status of a single monitor, including whether or not it is in the quorum.


AUTH subsystem
--------------
::

	$ ceph auth add <osd> <--in-file|-i> <path-to-osd-keyring>

Add auth keyring for an osd.  ::

	$ ceph auth list

Show auth key OSD subsystem.

PG subsystem
------------
::

	$ ceph -- pg dump [--format <format>]

Output the stats of all pgs. Valid formats are "plain" and "json",
and plain is the default. ::

	$ ceph -- pg dump_stuck inactive|unclean|stale [--format <format>] [-t|--threshold <seconds>]

Output the stats of all PGs stuck in the specified state.

``--format`` may be ``plain`` (default) or ``json``

``--threshold`` defines how many seconds "stuck" is (default: 300)

**Inactive** PGs cannot process reads or writes because they are waiting for an OSD
with the most up-to-date data to come back.

**Unclean** PGs contain objects that are not replicated the desired number
of times. They should be recovering.

**Stale** PGs are in an unknown state - the OSDs that host them have not
reported to the monitor cluster in a while (configured by
mon_osd_report_timeout). ::

	$ ceph pg <pgid> mark_unfound_lost revert

Revert "lost" objects to their prior state, either a previous version
or delete them if they were just created.


OSD subsystem
-------------
::

	$ ceph osd stat

Query osd subsystem status. ::

	$ ceph osd getmap -o file

Write a copy of the most recent osd map to a file. See
:doc:`osdmaptool </man/8/osdmaptool>`. ::

	$ ceph osd getcrushmap -o file

Write a copy of the crush map from the most recent osd map to
file. This is functionally equivalent to ::

	$ ceph osd getmap -o /tmp/osdmap
	$ osdmaptool /tmp/osdmap --export-crush file

::

	$ ceph osd dump [--format <format>]

Dump the osd map. Valid formats for -f are "plain" and "json". If no
--format option is given, the osd map is dumped as plain text. ::

	$ ceph osd tree [--format <format>]

Dump the osd map as a tree with one line per osd containing weight
and state. ::

	$ ceph osd crush set <id> <name> <weight> [<loc1> [<loc2> ...]]

Add or move a new item (osd) with the given id/name/weight at the specified
location. ::

	$ ceph osd crush remove <id>

Remove an existing item from the crush map. ::

        $ ceph osd crush move <id> <loc1> [<loc2> ...]

Move an existing bucket from one position in the hierarchy to another.  ::

	$ ceph osd crush reweight <name> <weight>

Set the weight of the item given by ``<name>`` to ``<weight>``. ::

	$ ceph osd cluster_snap <name>

Create a cluster snapshot. ::

	$ ceph osd lost [--yes-i-really-mean-it]

Mark an OSD as lost. This may result in permanent data loss. Use with caution. ::

	$ ceph osd create [<id>]

Create a new OSD. If no ID is given, a new ID is automatically selected
if possible. ::

	$ ceph osd rm [<id>...]

Remove the given OSD(s). ::

	$ ceph osd getmaxosd

Query the current max_osd parameter in the osd map. ::

	$ ceph osd setmap -i file

Import the given osd map. Note that this can be a bit dangerous,
since the osd map includes dynamic state about which OSDs are current
on or offline; only do this if you've just modified a (very) recent
copy of the map. ::

	$ ceph osd setcrushmap -i file

Import the given crush map. ::

	$ ceph osd setmaxosd

Set the max_osd parameter in the osd map. This is necessary when
expanding the storage cluster. ::

	$ ceph osd down N

Mark osdN down. ::

	$ ceph osd out N

Mark osdN out of the distribution (i.e. allocated no data). ::

	$ ceph osd in N

Mark osdN in the distribution (i.e. allocated data). ::

	$ ceph class list

List classes that are loaded in the ceph cluster. ::

	$ ceph osd pause
	$ ceph osd unpause

Set or clear the pause flags in the OSD map. If set, no IO requests
will be sent to any OSD. Clearing the flags via unpause results in
resending pending requests. ::

	$ ceph osd reweight N W

Set the weight of osdN to W. Two OSDs with the same weight will receive
roughly the same number of I/O requests and store approximately the
same amount of data. ::

	$ ceph osd reweight-by-utilization [threshold]

Reweights all the OSDs by reducing the weight of OSDs which are
heavily overused. By default it will adjust the weights downward on
OSDs which have 120% of the average utilization, but if you include
threshold it will use that percentage instead. ::

	$ ceph osd blacklist add ADDRESS[:source_port] [TIME]
	$ ceph osd blacklist rm ADDRESS[:source_port]

Adds/removes the address to/from the blacklist. When adding an address,
you can specify how long it should be blacklisted in seconds; otherwise
it will default to 1 hour. A blacklisted address is prevented from
connecting to any osd. Blacklisting is most often used to prevent a
laggy mds making bad changes to data on the osds.

These commands are mostly only useful for failure testing, as
blacklists are normally maintained automatically and shouldn't need
manual intervention. ::

	$ ceph osd pool mksnap POOL SNAPNAME
	$ ceph osd pool rmsnap POOL SNAPNAME

Creates/deletes a snapshot of a pool. ::

	$ ceph osd pool create POOL [pg_num [pgp_num]]
	$ ceph osd pool delete POOL
        $ ceph osd pool rename OLDNAME NEWNAME

Creates/deletes/renames a storage pool. ::

	$ ceph osd pool set POOL FIELD VALUE

Changes a pool setting. Valid fields are:

	* ``size``: Sets the number of copies of data in the pool.
	* ``crash_replay_interval``: The number of seconds to allow
	  clients to replay acknowledged but uncommited requests.
	* ``pg_num``: The placement group number.
	* ``pgp_num``: Effective number when calculating pg placement.
	* ``crush_ruleset``: rule number for mapping placement.

::

	$ ceph osd pool get POOL FIELD

Get the value of a pool setting. Valid fields are:

	* ``pg_num``: See above.
	* ``pgp_num``: See above.
	* ``lpg_num``: The number of local PGs.
	* ``lpgp_num``: The number used for placing the local PGs.

::

	$ ceph osd scrub N

Sends a scrub command to osdN. To send the command to all osds, use ``*``.
TODO: what does this actually do ::

	$ ceph osd repair N

Sends a repair command to osdN. To send the command to all osds, use ``*``.
TODO: what does this actually do

::

	$ ceph osd tell N bench [BYTES_PER_WRITE] [TOTAL_BYTES]

Runs a simple throughput benchmark against osdN, writing ``TOTAL_BYTES``
in write requests of ``BYTES_PER_WRITE`` each. By default, the test
writes 1 GB in total in 4-MB increments.

MDS subsystem
-------------

Change configuration parameters on a running mds. ::

	$ ceph mds tell <mds-id> injectargs '--<switch> <value> [--<switch> <value>]'

Example::

	$ ceph mds tell 0 injectargs '--debug_ms 1 --debug_mds 10'

Enables debug messages. ::

	$ ceph mds stat

Displays the status of all metadata servers.

.. todo:: ``ceph mds`` subcommands missing docs: set_max_mds, dump, getmap, stop, setmap


Mon subsystem
-------------

Show monitor stats::

	$ ceph mon stat
	2011-12-14 10:40:59.044395 mon <- [mon,stat]
	2011-12-14 10:40:59.057111 mon.1 -> 'e3: 5 mons at {a=10.1.2.3:6789/0,b=10.1.2.4:6789/0,c=10.1.2.5:6789/0,d=10.1.2.6:6789/0,e=10.1.2.7:6789/0}, election epoch 16, quorum 0,1,2,3' (0)

The ``quorum`` list at the end lists monitor nodes that are part of the current quorum.

This is also available more directly::

	$ ./ceph quorum_status
	2011-12-14 10:44:20.417705 mon <- [quorum_status]
	2011-12-14 10:44:20.431890 mon.0 -> '{ "election_epoch": 10,
	  "quorum": [
	        0,
	        1,
	        2],
	  "monmap": { "epoch": 1,
	      "fsid": "444b489c-4f16-4b75-83f0-cb8097468898",
	      "modified": "2011-12-12 13:28:27.505520",
	      "created": "2011-12-12 13:28:27.505520",
	      "mons": [
	            { "rank": 0,
	              "name": "a",
	              "addr": "127.0.0.1:6789\/0"},
	            { "rank": 1,
	              "name": "b",
	              "addr": "127.0.0.1:6790\/0"},
	            { "rank": 2,
	              "name": "c",
	              "addr": "127.0.0.1:6791\/0"}]}}' (0)

The above will block until a quorum is reached.

For a status of just the monitor you connect to (use ``-m HOST:PORT``
to select)::

	$ ./ceph mon_status
	2011-12-14 10:45:30.644414 mon <- [mon_status]
	2011-12-14 10:45:30.644632 mon.0 -> '{ "name": "a",
	  "rank": 0,
	  "state": "leader",
	  "election_epoch": 10,
	  "quorum": [
	        0,
	        1,
	        2],
	  "outside_quorum": [],
	  "monmap": { "epoch": 1,
	      "fsid": "444b489c-4f16-4b75-83f0-cb8097468898",
	      "modified": "2011-12-12 13:28:27.505520",
	      "created": "2011-12-12 13:28:27.505520",
	      "mons": [
	            { "rank": 0,
	              "name": "a",
	              "addr": "127.0.0.1:6789\/0"},
	            { "rank": 1,
	              "name": "b",
	              "addr": "127.0.0.1:6790\/0"},
	            { "rank": 2,
	              "name": "c",
	              "addr": "127.0.0.1:6791\/0"}]}}' (0)

A dump of the monitor state::

	$ ceph mon dump
	2011-12-14 10:43:08.015333 mon <- [mon,dump]
	2011-12-14 10:43:08.015567 mon.0 -> 'dumped monmap epoch 1' (0)
	epoch 1
	fsid 444b489c-4f16-4b75-83f0-cb8097468898
	last_changed 2011-12-12 13:28:27.505520
	created 2011-12-12 13:28:27.505520
	0: 127.0.0.1:6789/0 mon.a
	1: 127.0.0.1:6790/0 mon.b
	2: 127.0.0.1:6791/0 mon.c

