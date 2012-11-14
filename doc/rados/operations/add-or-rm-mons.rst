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
consensus about the master cluster map, which requires a majority of
monitors running to establish a quorum for consensus about the cluster map
(e.g., 1; 3 out of 5; 4 out of 6; etc.).

Since monitors are light-weight, it is possible to run them on the same 
host as an OSD; however, we recommend running them on separate hosts. 

.. important:: A *majority* of monitors in your cluster must be able to 
   reach each other in order to establish a quorum.

Deploy your Hardware
--------------------

If you are adding a new host when adding a new monitor,  see `Hardware
Recommendations`_ for details on minimum recommendations for monitor hardware.
To add a monitor host to your cluster, first make sure you have an up-to-date
version of Linux installed (typically Ubuntu 12.04 precise). 

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
and monitor keyring, and adds a ``ceph-mon`` daemon to your cluster.  If
this results in only two monitor daemons, you may add more monitors by
repeating this procedure until you have a sufficient number of ``ceph-mon`` 
daemons to achieve a quorum.

#. Create the default directory on your new monitor. :: 

	ssh {new-mon-host}
	sudo mkdir /var/lib/ceph/mon/ceph-{mon-letter}

#. Create a temporary directory ``{tmp}`` to keep the files needed during 
   this process. This directory should be different from monitor's default 
   directory created in the previous step, and can be removed after all the 
   steps are taken. :: 

	mkdir {tmp}

#. Retrieve the keyring for your monitors, where ``{tmp}`` is the path to 
   the retrieved keyring, and ``{filename}`` is the name of the file containing
   the retrieved monitor key. :: 

	ceph auth get mon. -o {tmp}/{filename}

#. Retrieve the monitor map, where ``{tmp}`` is the path to 
   the retrieved monitor map, and ``{filename}`` is the name of the file 
   containing the retrieved monitor monitor map. :: 

	ceph mon getmap -o {tmp}/{filename}

#. Prepare the monitor's data directory created in the first step. You must 
   specify the path to the monitor map so that you can retrieve the 
   information about a quorum of monitors and their ``fsid``. You must also 
   specify a path to the monitor keyring:: 

	sudo ceph-mon -i {mon-letter} --mkfs --monmap {tmp}/{filename} --keyring {tmp}/{filename}	
	

#. Add a ``[mon.{letter}]`` entry for your new monitor in your ``ceph.conf`` file. ::

	[mon.c]
		host = new-mon-host
		addr = ip-addr:6789

#. Add the new monitor to the list of monitors for you cluster (runtime). This enables 
   other nodes to use this monitor during their initial startup. ::

	ceph mon add <name> <ip>[:<port>]

#. Start the new monitor and it will automatically join the cluster.
   The daemon needs to know which address to bind to, either via
   ``--public-addr {ip:port}`` or by setting ``mon addr`` in the
   appropriate section of ``ceph.conf``.  For example::

	ceph-mon -i newname --public-addr {ip:port}


Removing Monitors
=================

When you remove monitors from a cluster, consider that Ceph monitors use 
PAXOS to establish consensus about the master cluster map. You must have 
a sufficient number of monitors to establish a quorum for consensus about 
the cluster map.

Removing a Monitor (Manual)
---------------------------

This procedure removes a ``ceph-mon`` daemon from your cluster.   If this
procedure results in only two monitor daemons, you may add or remove another
monitor until you have a number of ``ceph-mon`` daemons that can achieve a 
quorum.

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
