.. index:: control, commands

=================
 Control commands
=================

Monitor commands
----------------

Monitor commands are issued using the ceph utility (in versions before
Dec08 it was called cmonctl)::

	$ ceph [-m monhost] command

where the command is usually of the form::

	$ ceph subsystem command

System commands
---------------

::

	$ ceph stop

Cleanly shuts down the cluster.  ::

	$ ceph -s

Shows an overview of the current status of the cluster.  ::

	$ ceph -w

Shows a running summary of the status of the cluster, and major events.

AUTH subsystem
--------------
::

	$ ceph auth add <osd> <--in-file|-i> <path-to-osd-keyring>

Add auth keyring for an osd.  ::

	$ ceph auth list

Show auth key OSD subsystem.

OSD subsystem
-------------
::

	$ ceph osd stat

Query osd subsystem status. ::

	$ ceph osd getmap -o file

Write a copy of the most recent osd map to a file. See osdmaptool. ::

	$ ceph osd getcrushmap -o file

Write a copy of the crush map from the most recent osd map to
file. This is functionally equivalent to ::

	$ ceph osd getmap -o /tmp/osdmap
	$ osdmaptool /tmp/osdmap --export-crush file
::

	$ ceph osd dump [--format format>]

Dump the osd map. Valid formats for -f are "plain" and "json". If no
--format option is given, the osd map is dumped as plain text. ::

	$ ceph osd tree [--format format]

Dump the osd map as a tree with one line per osd containing weight
and state. ::

	$ ceph osd crush add <id> <name> <weight> [<loc1> [<loc2> ...]]

Add a new item with the given id/name/weight at the specified
location. ::

	$ ceph osd crush remove <id>

Remove an existing item from the crush map. ::

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

TODO ::

	$ ceph osd reweight N W

Sets the weight of osdN to W. ::

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

	$ ceph osd pool create POOL
	$ ceph osd pool delete POOL

Creates/deletes a storage pool. ::

	$ ceph osd pool set POOL FIELD VALUE

Changes a pool setting. Valid fields are:

	* ``size``: Sets the number of copies of data in the pool.
	* ``pg_num``: TODO
	* ``pgp_num``: TODO

::

	$ ceph osd scrub N

Sends a scrub command to osdN. To send the command to all osds, use ``*``.
TODO: what does this actually do ::

	$ ceph osd repair N

Sends a repair command to osdN. To send the command to all osds, use ``*``.
TODO: what does this actually do

MDS subsystem
-------------

Change configuration parameters on a running mds. ::

	$ ceph mds tell <mds-id> injectargs '--<switch> <value> [--<switch> <value>]'

Example::

	$ ceph mds tell 0 injectargs '--debug_ms 1 --debug_mds 10'

Enables debug messages. ::

	$ ceph mds stat

Displays the status of all metadata servers.

set_max_mds: TODO
