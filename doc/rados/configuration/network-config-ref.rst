=================================
 Network Configuration Reference
=================================

Network configuration is critical for building a high performance  :term:`Ceph
Storage Cluster`. The Ceph Storage Cluster does not perform  request routing or
dispatching on behalf of the :term:`Ceph Client`. Instead, Ceph Clients make
requests directly to Ceph OSD Daemons. Ceph OSD Daemons perform data replication
on behalf of Ceph Clients, which means replication and other factors impose
additional loads on Ceph Storage Cluster networks.

Our Quick Start configurations provide a trivial Ceph configuration file that
sets monitor IP addresses and daemon host names only. Unless you specify a
cluster network, Ceph assumes a single "public" network. Ceph functions just
fine with a public network only, but you may see significant performance
improvement with a second "cluster" network in a large cluster.

It is possible to run a Ceph Storage Cluster with two networks: a public
(client, front-side) network and a cluster (private, replication, back-side)
network.  However, this approach
complicates network configuration (both hardware and software) and does not usually
have a significant impact on overall performance.  For this reason, we recommend
that for resilience and capacity dual-NIC systems either active/active bond
these interfaces or implement a layer 3 multipath strategy with eg. FRR.

If, despite the complexity, one still wishes to use two networks, each
:term:`Ceph Node` will need to have more than one network interface or VLAN. See `Hardware
Recommendations - Networks`_ for additional details.

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


IP Tables
=========

By default, daemons `bind`_ to ports within the ``6800:7568`` range. You may
configure this range at your discretion. Before configuring your IP tables,
check the default ``iptables`` configuration.

.. prompt:: bash $

   sudo iptables -L

Some Linux distributions include rules that reject all inbound requests
except SSH from all network interfaces. For example:: 

	REJECT all -- anywhere anywhere reject-with icmp-host-prohibited

You will need to delete these rules on both your public and cluster networks
initially, and replace them with appropriate rules when you are ready to 
harden the ports on your Ceph Nodes.


Monitor IP Tables
-----------------

Ceph Monitors listen on ports ``3300`` and ``6789`` by
default. Additionally, Ceph Monitors always operate on the public
network. When you add the rule using the example below, make sure you
replace ``{iface}`` with the public network interface (e.g., ``eth0``,
``eth1``, etc.), ``{ip-address}`` with the IP address of the public
network and ``{netmask}`` with the netmask for the public network. :

.. prompt:: bash $

   sudo iptables -A INPUT -i {iface} -p tcp -s {ip-address}/{netmask} --dport 6789 -j ACCEPT


MDS and Manager IP Tables
-------------------------

A :term:`Ceph Metadata Server` or :term:`Ceph Manager` listens on the first 
available port on the public network beginning at port 6800. Note that this 
behavior is not deterministic, so if you are running more than one OSD or MDS
on the same host, or if you restart the daemons within a short window of time,
the daemons will bind to higher ports. You should open the entire 6800-7568
range by default.  When you add the rule using the example below, make sure
you replace ``{iface}`` with the public network interface (e.g., ``eth0``,
``eth1``, etc.), ``{ip-address}`` with the IP address of the public network
and ``{netmask}`` with the netmask of the public network.

For example:

.. prompt:: bash $

   sudo iptables -A INPUT -i {iface} -m multiport -p tcp -s {ip-address}/{netmask} --dports 6800:7568 -j ACCEPT


OSD IP Tables
-------------

By default, Ceph OSD Daemons `bind`_ to the first available ports on a Ceph Node
beginning at port 6800.  Note that this behavior is not deterministic, so if you
are running more than one OSD or MDS on the same host, or if you restart the
daemons within a short window of time, the daemons will bind to higher ports.
Each Ceph OSD Daemon on a Ceph Node may use up to four ports:

#. One for talking to clients and monitors.
#. One for sending data to other OSDs.
#. Two for heartbeating on each interface.

.. ditaa::
              /---------------\
              |      OSD      |
              |           +---+----------------+-----------+
              |           | Clients & Monitors | Heartbeat |
              |           +---+----------------+-----------+
              |               |
              |           +---+----------------+-----------+
              |           | Data Replication   | Heartbeat |
              |           +---+----------------+-----------+
              | cCCC          |
              \---------------/

When a daemon fails and restarts without letting go of the port, the restarted
daemon will bind to a new port. You should open the entire 6800-7568 port range
to handle this possibility.

If you set up separate public and cluster networks, you must add rules for both
the public network and the cluster network, because clients will connect using
the public network and other Ceph OSD Daemons will connect using the cluster
network. When you add the rule using the example below, make sure you replace
``{iface}`` with the network interface (e.g., ``eth0``, ``eth1``, etc.),
``{ip-address}`` with the IP address and ``{netmask}`` with the netmask of the
public or cluster network. For example:

.. prompt:: bash $

   sudo iptables -A INPUT -i {iface}  -m multiport -p tcp -s {ip-address}/{netmask} --dports 6800:7568 -j ACCEPT

.. tip:: If you run Ceph Metadata Servers on the same Ceph Node as the 
   Ceph OSD Daemons, you can consolidate the public network configuration step. 


Ceph Networks
=============

To configure Ceph networks, you must add a network configuration to the
``[global]`` section of the configuration file. Our 5-minute Quick Start
provides a trivial Ceph configuration file that assumes one public network
with client and server on the same network and subnet. Ceph functions just fine
with a public network only. However, Ceph allows you to establish much more
specific criteria, including multiple IP network and subnet masks for your
public network. You can also establish a separate cluster network to handle OSD
heartbeat, object replication and recovery traffic. Don't confuse the IP
addresses you set in your configuration with the public-facing IP addresses
network clients may use to access your service. Typical internal IP networks are
often ``192.168.0.0`` or ``10.0.0.0``.

.. tip:: If you specify more than one IP address and subnet mask for
   either the public or the cluster network, the subnets within the network
   must be capable of routing to each other. Additionally, make sure you
   include each IP address/subnet in your IP tables and open ports for them
   as necessary.

.. note:: Ceph uses `CIDR`_ notation for subnets (e.g., ``10.0.0.0/24``).

When you have configured your networks, you may restart your cluster or restart
each daemon. Ceph daemons bind dynamically, so you do not have to restart the
entire cluster at once if you change your network configuration.


Public Network
--------------

To configure a public network, add the following option to the ``[global]``
section of your Ceph configuration file. 

.. code-block:: ini

	[global]
		# ... elided configuration
		public_network = {public-network/netmask}

.. _cluster-network:

Cluster Network
---------------

If you declare a cluster network, OSDs will route heartbeat, object replication
and recovery traffic over the cluster network. This may improve performance
compared to using a single network. To configure a cluster network, add the
following option to the ``[global]`` section of your Ceph configuration file. 

.. code-block:: ini

	[global]
		# ... elided configuration
		cluster_network = {cluster-network/netmask}

We prefer that the cluster network is **NOT** reachable from the public network
or the Internet for added security.

Ceph Daemons
============

Monitor daemons are each configured to bind to a specific IP address.  These
addresses are normally configured by your deployment tool.  Other components
in the Ceph cluster discover the monitors via the ``mon host`` configuration
option, normally specified in the ``[global]`` section of the ``ceph.conf`` file.

.. code-block:: ini

     [global]
         mon_host = 10.0.0.2, 10.0.0.3, 10.0.0.4

The ``mon_host`` value can be a list of IP addresses or a name that is
looked up via DNS.  In the case of a DNS name with multiple A or AAAA
records, all records are probed in order to discover a monitor.  Once
one monitor is reached, all other current monitors are discovered, so
the ``mon host`` configuration option only needs to be sufficiently up
to date such that a client can reach one monitor that is currently online.

The MGR, OSD, and MDS daemons will bind to any available address and
do not require any special configuration.  However, it is possible to
specify a specific IP address for them to bind to with the ``public
addr`` (and/or, in the case of OSD daemons, the ``cluster addr``)
configuration option.  For example,

.. code-block:: ini

	[osd.0]
		public_addr = {host-public-ip-address}
		cluster_addr = {host-cluster-ip-address}

.. topic:: One NIC OSD in a Two Network Cluster

   Generally, we do not recommend deploying an OSD host with a single network interface in a 
   cluster with two networks. However, you may accomplish this by forcing the 
   OSD host to operate on the public network by adding a ``public_addr`` entry
   to the ``[osd.n]`` section of the Ceph configuration file, where ``n`` 
   refers to the ID of the OSD with one network interface. Additionally, the public
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
addresses or override ``public_network`` settings using the ``public_addr``
setting for a specific daemon.

.. confval:: public_network_interface
.. confval:: public_network
.. confval:: public_addr

Cluster Network
---------------

The cluster network configuration allows you to declare a cluster network, and
specifically define IP addresses and subnets for the cluster network. You may
specifically assign static IP  addresses or override ``cluster_network``
settings using the ``cluster_addr`` setting for specific OSD daemons.


.. confval:: cluster_network_interface 
.. confval:: cluster_network
.. confval:: cluster_addr

Bind
----

Bind settings set the default port ranges Ceph OSD and MDS daemons use. The
default range is ``6800:7568``. Ensure that your `IP Tables`_ configuration
allows you to use the configured port range.

You may also enable Ceph daemons to bind to IPv6 addresses instead of IPv4
addresses.

.. confval:: ms_bind_port_min
.. confval:: ms_bind_port_max
.. confval:: ms_bind_ipv4
.. confval:: ms_bind_ipv6
.. confval:: public_bind_addr

TCP
---

Ceph disables TCP buffering by default.

.. confval:: ms_tcp_nodelay
.. confval:: ms_tcp_rcvbuf

General Settings
----------------

.. confval:: ms_type
.. confval:: ms_async_op_threads
.. confval:: ms_initial_backoff
.. confval:: ms_max_backoff
.. confval:: ms_die_on_bad_msg
.. confval:: ms_dispatch_throttle_bytes
.. confval:: ms_inject_socket_failures


.. _Scalability and High Availability: ../../../architecture#scalability-and-high-availability
.. _Hardware Recommendations - Networks: ../../../start/hardware-recommendations#networks
.. _hardware recommendations: ../../../start/hardware-recommendations
.. _Monitor / OSD Interaction: ../mon-osd-interaction
.. _Message Signatures: ../auth-config-ref#signatures
.. _CIDR: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing
.. _Nagle's Algorithm: https://en.wikipedia.org/wiki/Nagle's_algorithm
