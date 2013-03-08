=================================
 Network Configuration Reference
=================================

Network configuration is critical for building a high performance Ceph cluster.
The Ceph cluster does not perform request routing or dispatching on behalf of
the client. Instead, Ceph clients (i.e., block device, CephFS, REST gateway)
make requests directly to OSDs. Ceph OSDs perform data replication on behalf of
clients, which means replication and other factors impose additional loads on
Ceph cluster networks.

Our 5-minute Quick Start provides a trivial `Ceph configuration file`_ that sets
monitor IP addresses and daemon host names only. The quick start configuration
assumes a single "public" network. Ceph functions just fine with a public
network only, but you may see significant performance improvement with a second
network in a large cluster.

We recommend running a Ceph cluster with two networks: a public (front-side)
network and a cluster (back-side) network. To support two networks, your hosts
need to have more than one NIC. See `Hardware Recommendations -  Networks`_ for
additional details.

.. ditaa::
                               +-------------+
                               | Ceph Client |
                               +----*--*-----+
                                    |  ^
                            Request |  : Response
                                    v  |
 /----------------------------------*--*-------------------------------------\
 |                              Public Network                               |
 \---*--*------------*--*-------------*--*------------*--*------------*--*---/
     ^  ^            ^  ^             ^  ^            ^  ^            ^  ^
     |  |            |  |             |  |            |  |            |  |
     |  :            |  :             |  :            |  :            |  :
     v  v            v  v             v  v            v  v            v  v
 +---*--*---+    +---*--*---+     +---*--*---+    +---*--*---+    +---*--*---+
 | Ceph MON |    | Ceph MDS |     | Ceph OSD |    | Ceph OSD |    | Ceph OSD |
 +----------+    +----------+     +---*--*---+    +---*--*---+    +---*--*---+
                                      ^  ^            ^  ^            ^  ^
     The cluster network relieves     |  |            |  |            |  |
     OSD replication and heartbeat    |  :            |  :            |  :
     traffic from the public network. v  v            v  v            v  v
 /------------------------------------*--*------------*--*------------*--*---\
 |   cCCC                      Cluster Network                               |
 \---------------------------------------------------------------------------/


There are several reasons to consider operating two separate networks:

#. **Peformance:** OSDs handle data replication for the clients. When OSDs 
   replicate data more than once, the network load between OSDs easily dwarfs 
   the network load between clients and the Ceph cluster. This can introduce 
   latency and create a performance problem. Recovery and rebalacing can 
   also introduce significant latency on the public network. See `How Ceph 
   Scales`_ for additional details on how Ceph replicates data. See 
   `Monitor / OSD Interaction`_  for details on heartbeat traffic.

#. **Security**: While most people are generally civil, a very tiny segment of 
   the population likes to engage in what's known as a Denial of Service (DoS) 
   attack. When traffic between OSDs gets disrupted, placement groups may no
   longer reflect an ``active + clean`` state, which may prevent users from 
   reading and writing data. A great way to defeat this type of attack is to 
   maintain a completely separate cluster network that doesn't connect directly 
   to the internet. Also, consider using `Message Signatures`_ to defeat 
   spoofing attacks.


IP Tables
=========

Before configuring your IP tables, check the default ``iptables`` configuration.
::

	sudo iptables -L

Some Linux distributions include rules that reject all inbound requests
except SSH from all network interfaces. For example:: 

	REJECT all -- anywhere anywhere reject-with icmp-host-prohibited

You will need to delete these rules on both your public and cluster networks
initially, and replace them with appropriate rules when you are ready to 
harden the ports on your cluster hosts.


Monitor IP Tables
-----------------

Monitors listen on port 6789 by default. Additionally, monitors always operate
on the public network. When you add the rule using the example below, make
sure you replace ``{iface}`` with the public network interface (e.g., ``eth0``,
``eth1``, etc.), ``{ip-address}`` with  the IP address of the public network and
``{netmask}`` with the netmask for the public network. ::

   sudo iptables -A INPUT -i {iface} -p tcp -s {ip-address}/{netmask} --dport 6789 -j ACCEPT


MDS IP Tables
-------------

Metadata servers listen on the first available port on the public network
beginning at port 6800. Ensure that you open one port beginning at port 6800 for
each metadata server that runs on the host. When you add the rule using the
example below, make sure you replace ``{iface}`` with the public network
interface (e.g., ``eth0``, ``eth1``, etc.), ``{ip-address}`` with the IP address
of the public network and ``{netmask}`` with the netmask of the public network.

For example:: 

	sudo iptables -A INPUT -i {iface} -m multiport -p tcp -s {ip-address}/{netmask} --dports 6800:6810 -j ACCEPT


OSD IP Tables
-------------

OSDs servers listen on the first available port beginning at port 6800. Ensure
that you open one port beginning at port 6800 for each OSD server that runs on
the host. Ports are host-specific, so you don't need to open any more ports than
the number of Ceph daemons running on that host. You may consider opening a few
additional ports in case a daemon fails and restarts without letting go of the
port such that the restarted daemon binds to a new port. 

If you set up separate public and cluster networks, you must add rules for both
the public network and the cluster network, because clients will connect using
the public network and other OSDs will connect using the cluster network. When
you add the rule using the example below, make sure you replace ``{iface}`` with
the network interface (e.g., ``eth0``, ``eth1``, etc.), ``{ip-address}`` with
the IP address and ``{netmask}`` with the netmask of the public or cluster
network. For example:: 

	sudo iptables -A INPUT -i {iface}  -m multiport -p tcp -s {ip-address}/{netmask} --dports 6800:6810 -j ACCEPT


.. tip:: If you run metadata servers on the same host as the OSDs,
   you can consolidate the public network configuration step. Ensure
   you open a port for each daemon.



Ceph Networks
=============

To configure Ceph networks, you must add a network configuration to the
``[global]`` section of the configuration file. Our 5-minute Quick Start
provides a trivial `Ceph configuration file`_ that assumes one public network
with client and server on the same network and subnet. Ceph functions just fine
with a public network only. However, Ceph  allows you to establish much more
specific criteria, including multiple network IP addresses and subnet masks
for your public network. You can also establish a separate cluster network
to handle OSD heartbeat, object replication and recovery traffic. 

.. tip:: If you specify more than one IP address and subnet mask for
   either the public or the cluster network, the subnets within the network
   must be capable of routing to each other. Additionally, make sure you
   include each IP address/subnet in your IP tables and open ports for them
   as necessary.

.. note:: Ceph uses `CIDR`_ notation for subnets (e.g., ``10.20.30.40/24``).


Public Network
--------------

To configure a public network, add the following option to the ``[global]``
section of your Ceph configuration file. 

.. code-block:: ini

	[global]
		...
		public network = {public-network-ip-address/netmask}


Cluster Network
---------------

If you declare a cluster network, OSDs will route heartbeat, object replication
and recovery traffic over the cluster network. This may improve performance
compared to using a single network. To configure a cluster network, add the
following option to the ``[global]`` section of your Ceph configuration file. 

.. code-block:: ini

	[global]
		...
		cluster network = {enter cluster-network-ip-address/netmask}

We prefer that the cluster network is **NOT** reachable from the public network
or the Internet for added security.


Ceph Daemons
============

Ceph has one network configuration requirement that applies to all daemons: the
Ceph configuration file **MUST** specify the ``host`` for each daemon. Ceph also
requires that a Ceph configuration file specify the monitor IP address and its
port.

.. important:: Some deployment tools (e.g., ``ceph-deploy``, Chef) may create a
   configuration file for you. **DO NOT** set these values if the deployment 
   tool does it for you.

.. tip:: The ``host`` setting is the short name of the host (i.e., not 
   an fqdn). It is **NOT** an IP address either.  Enter ``hostname -s`` on 
   the command line to retrieve the name of the host.


.. code-block:: ini

	[mon.a]
	
		host = {hostname}
		mon addr = {ip-address}:6789

	[osd.0]
		host = {hostname}


You do not have to set the host IP address for a daemon. If you have a static IP
configuration and both public and cluster networks running, the Ceph
configuration file may specify the IP address of the host for each daemon. To
set a static IP address for a daemon, the following option(s) should appear in
the daemon instance sections of your ``ceph.conf`` file.

.. code-block:: ini

	[osd.0]
		public addr = {host-public-ip-address}
		cluster addr = {host-cluster-ip-address}


.. topic:: One NIC OSD in a Two Network Cluster

   Generally, we do not recommend deploying an OSD host with a single NIC in a 
   cluster with two networks. However, you may accomplish this by forcing the 
   OSD host to operate on the public network by adding a ``public addr`` entry
   to the ``[osd.n]`` section of the Ceph configuration file, where ``n`` 
   refers to the number of the OSD with one NIC. Additionally, the public
   network and cluster network must be able to route traffic to each other, 
   which we don't recommend for security reasons.


Network Config Settings
=======================

Network configuration settings are not required. Ceph assumes a public network
with all hosts operating on it unless you specifically configure a cluster 
network.

Public Network
--------------

The public network configuration allows you specifically define IP addresses
and subnets for the public network. You may specifically assign static IP 
addresses or override ``public network`` settings using the ``public addr``
setting for a specific daemon.

``public network``

:Description: The IP address and netmask of the public (front-side) network 
              (e.g., ``10.20.30.40/24``). Set in ``[global]``. You may specify
              comma-delimited subnets.

:Type: ``{ip-address}/{netmask} [, {ip-address}/{netmask}]``
:Required: No
:Default: N/A


``public addr``

:Description: The IP address for the public (front-side) network. 
              Set for each daemon.

:Type: IP Address
:Required: No
:Default: N/A



Cluster Network
---------------

The cluster network configuration allows you to declare a cluster network, and
specifically define IP addresses and subnets for the cluster network. You may
specifically assign static IP  addresses or override ``cluster network``
settings using the ``cluster addr`` setting for specific OSD daemons.


``cluster network``

:Description: The IP address and netmask of the cluster (back-side) network 
              (e.g., ``10.20.30.41/24``).  Set in ``[global]``. You may specify
              comma-delimited subnets.

:Type: ``{ip-address}/{netmask} [, {ip-address}/{netmask}]``
:Required: No
:Default: N/A


``cluster addr``

:Description: The IP address for the cluster (back-side) network. 
              Set for each daemon.

:Type: Address
:Required: No
:Default: N/A


Hosts
-----

Ceph expects at least one monitor declared in the Ceph configuration file, with
a ``mon host`` setting under each declared monitor. Ceph expects a ``host``
setting under each declared metadata server and OSD in the  Ceph configuration
file.


``mon host``

:Description: A list of ``{hostname}:{port}`` entries that clients can use to 
              connect to a Ceph monitor. If not set, Ceph searches ``[mon.*]`` 
              sections. 

:Type: String
:Required: No
:Default: N/A


``host``

:Description: The hostname. Use this setting for specific daemon instances 
              (e.g., ``[osd.0]``).

:Type: String
:Required: Yes, for daemon instances.
:Default: ``localhost``

.. tip:: Do not use ``localhost``. To get your host name, execute 
         ``hostname -s`` on your command line and use the name of your host 
         (to the first period, not the fully-qualified domain name).

.. important:: You should not specify any value for ``host`` when using a third
               party deployment system that retrieves the host name for you.




.. _How Ceph Scales: ../../../architecture#how-ceph-scales
.. _Hardware Recommendations - Networks: ../../../install/hardware-recommendations#networks
.. _Ceph configuration file: ../../../start/quick-start/#add-a-configuration-file
.. _hardware recommendations: ../../../install/hardware-recommendations
.. _Monitor / OSD Interaction: ../mon-osd-interaction
.. _Message Signatures: ../auth-config-ref#signatures
.. _CIDR: http://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing