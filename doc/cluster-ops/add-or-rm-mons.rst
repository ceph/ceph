==========================
 Adding/Removing Monitors
==========================

When you have a cluster up and running, you may add or remove monitors
from the cluster at runtime.

Adding Monitors
===============

Ceph monitors are light-weight processes that maintain a master copy of the 
cluster map. You can run a cluster with 1 monitor. We recommend at least 3 
monitors for a production cluster. Ceph monitors use PAXOS to establish 
consensus about the master cluster map, so you must have an odd number of
monitors to establish a quorum for consensus about the cluster map. 

Since monitors are light-weight, it is possible to run them on the same 
host as an OSD; however, we recommend running them on separate hosts. 

.. important:: You must have an odd number of monitors in your cluster
   to establish a quorum.

Deploy your Hardware
--------------------

If you are adding a new host when adding a new monitor, 
see `Hardware Recommendations`_ for details on minimum recommendations
for monitor hardware. To add a monitor host to your cluster, first make sure you have 
an up-to-date version of Linux installed (typically Ubuntu 12.04 precise). 

Add your monitor host to a rack in your cluster, connect it to the network
and ensure that it has network connectivity.

.. _Hardware Recommendations: ../../install/hardware-recommendations

Install the Required Software
-----------------------------

For manually deployed clusters, you must install Ceph packages
manually. See `Installing Debian/Ubuntu Packages`_ for details.
You should configure SSH to a user with password-less authentication
and root permissions.

.. _Installing Debian/Ubuntu Packages: ../../install/debian

For clusters deployed with Chef, create a `chef user`_, `configure
SSH keys`_, `install Ruby`_ and `install the Chef client`_ on your host. See 
`Installing Chef`_ for details.

.. _chef user: ../../install/chef#createuser
.. _configure SSH keys: ../../install/chef#genkeys
.. _install the Chef client: ../../install/chef#installchef
.. _Installing Chef: ../../install/chef
.. _install Ruby: ../../install/chef#installruby

.. _adding-mon:

Adding a Monitor (Manual)
-------------------------

This procedure creates a ``ceph-mon`` data directory, retrieves the monitor map
and monitor keyring, and adds a ``ceph-mon`` daemon to your cluster.  If you
this results in an even number of daemons,  you may add another monitor by
repeating this procedure until you have an odd number of ``ceph-mon`` daemons.

#. Create the default directory on your new monitor. :: 

	ssh {new-mon-host}
	sudo mkdir /var/lib/ceph/mon/ceph-{mon-letter}

#. Retrieve the keyring for your monitors, where ``{path}`` is the path to 
   the retrieved keyring, and ``{filename}`` is the name of the file containing
   the retrieved monitor key. :: 

	ceph auth get mon. -o {path}/{filename}

#. Retrieve the monitor map, where ``{path}`` is the path to 
   the retrieved monitor map, and ``{filename}`` is the name of the file containing
   the retrieved monitor monitor map. :: 

	ceph mon getmap -o {path}/{filename}

#. Prepare the data directory you just created. You must specify
   the path to the monitor map so that you can retrieve the information
   about a quorum of monitors and their ``fsid``. You must also specify
   a path to the monitor keyring:: 

	sudo ceph-mon -i {mon-letter} --mkfs --monmap {path}/{filename} --keyring {path}/{filename}	
	

#. Add a ``[mon.{letter}]`` entry for your new monitor in your ``ceph.conf`` file. ::

	[mon.c]
		host = new-mon-host
		addr = ip-addr:6789

#. Add the new monitor to the list of monitors for you cluster (runtime). This enables 
   other nodes to use this monitor during their initial startup. ::

	ceph mon add <name> <ip>[:<port>]\n";

#. Start the new monitor and it will automatically join the cluster.
   The daemon needs to know which address to bind to, either via
   ``--public-addr {ip:port}`` or by setting ``mon addr`` in the
   appropriate section of ``ceph.conf``.  For example::

	ceph-mon -i newname --public-addr {ip:port}


Removing Monitors
=================

When you remove monitors from a cluster, Ceph monitors use PAXOS to establish 
consensus about the master cluster map, so you must have an odd number of
monitors to establish a quorum for consensus about the cluster map.

Removing a Monitor (Manual)
---------------------------

This procedure removes a ``ceph-mon`` daemon from your cluster.   If this
procedure results in an even number of daemons, you may add or remove another
monitor  until you have an odd number of ``ceph-mon`` daemons.

#. Stop the monitor. ::

	service ceph -a stop mon.{mon-letter}
	
#. Remove the monitor from the cluster. ::

	ceph mon remove {mon-letter}	
	
#. Remove the monitor entry from ``ceph.conf``. 


Removing Monitors from an Unhealthy Cluster
-------------------------------------------

This procedure removes a ``ceph-mon`` daemon from an unhealhty cluster--i.e., 
a cluster that has placement groups that are persistently not ``active + clean``.


#. Identify a surviving monitor. :: 

	ceph mon dump

#. Navigate to a surviving monitor's ``monmap`` directory. :: 

	ssh {mon-host}
	cd /var/lib/ceph/mon/ceph-{mon-letter}/monmap

#. List the directory contents and identify the last commmitted map.
   Directory contents will show a numeric list of maps. ::

	ls 	
	1  2  3  4  5  first_committed  last_committed  last_pn  latest


#. Identify the most recently committed map. ::

	sudo cat last_committed

#. Copy the most recently committed file to a temporary directory. ::

	cp /var/lib/ceph/mon/ceph-{mon-letter}/monmap/{last_committed} /tmp/surviving_map
	
#. Remove the non-surviving monitors. 	For example, if you have three monitors, 
   ``mon.a``, ``mon.b``, and ``mon.c``, where only ``mon.a`` will survive, follow 
   the example below:: 

	monmaptool /tmp/surviving_map --rm {mon-letter}
	#for example
	monmaptool /tmp/surviving_map --rm b
	monmaptool /tmp/surviving_map --rm c
	
#. Stop all monitors. ::

	service ceph -a stop mon
	
#. Inject the surviving map with the removed monitors into the surviving monitors. 
   For example, to inject a map into monitor ``mon.a``, follow the example below:: 

	ceph-mon -i {mon-letter} --inject-monmap {map-path}
	#for example
	ceph-mon -i a --inject-monmap /etc/surviving_map
