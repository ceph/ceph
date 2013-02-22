==================
 Configuring Ceph
==================

When you start the Ceph service, the initialization process activates a series
of daemons that run in the background. The hosts in a typical Ceph cluster run
at least one of four daemons:

- Object Storage Device (``ceph-osd``)
- Monitor (``ceph-mon``)
- Metadata Server (``ceph-mds``)
- Ceph Gateway (``radosgw``)

For your convenience, each daemon has a series of default values (*i.e.*, many
are set by  ``ceph/src/common/config_opts.h``). You may override these settings
with a Ceph configuration file.

.. _ceph-conf-file:

The Configuration File
======================

When you start a Ceph cluster, each daemon looks for a Ceph configuration file
(i.e., ``ceph.conf`` by default) that provides the cluster's configuration
settings. For manual deployments, you need to create a Ceph configuration file.
For third party tools that create configuration files for you (*e.g.*, Chef),
you may use the information contained herein as a reference. The Ceph
Configuration file defines:

- Authentication settings
- Cluster membership
- Host names
- Host addresses
- Paths to keyrings
- Paths to journals
- Paths to data
- Other runtime options

The default ``ceph.conf`` locations in sequential order include:

#. ``$CEPH_CONF`` (*i.e.,* the path following the ``$CEPH_CONF`` environment variable)
#. ``-c path/path``  (*i.e.,* the ``-c`` command line argument)
#. ``/etc/ceph/ceph.conf``
#. ``~/.ceph/config``
#. ``./ceph.conf`` (*i.e.,* in the current working directory)


The Ceph configuration file uses an *ini* style syntax. You can add comments 
by preceding comments with a semi-colon (;) or a pound sign (#).  For example:

.. code-block:: ini

	# <--A number (#) sign precedes a comment.
	; A comment may be anything. 
	# Comments always follow a semi-colon (;) or a pound (#) on each line.
	# The end of the line terminates a comment.
	# We recommend that you provide comments in your configuration file(s).

.. _ceph-conf-settings:

Config Sections 
===============

The configuration file can configure all daemons in a cluster, or all daemons of
a particular type. To configure a series of daemons, the settings must be
included under the processes that will receive the configuration as follows: 

``[global]``

:Description: Settings under ``[global]`` affect all daemons in a Ceph cluster. 
:Example: ``auth supported = cephx``

``[osd]``

:Description: Settings under ``[osd]`` affect all ``ceph-osd`` daemons in the cluster.
:Example: ``osd journal size = 1000``

``[mon]``

:Description: Settings under ``[mon]`` affect all ``ceph-mon`` daemons in the cluster.
:Example: ``mon addr = 10.0.0.101:6789``


``[mds]``

:Description: Settings under ``[mds]`` affect all ``ceph-mds`` daemons in the cluster. 
:Example: ``host = myserver01``

``[client]``

:Description: Settings under ``[client]`` affect all clients (e.g., mounted CephFS filesystems, mounted block devices, etc.)
:Example: ``log file = /var/log/ceph/radosgw.log``

Global settings affect all instances of all daemon in the cluster. Use the ``[global]`` 
setting for values that are common for all daemons in the cluster. You can override each
``[global]`` setting by:

#. Changing the setting in a particular process type (*e.g.,* ``[osd]``, ``[mon]``, ``[mds]`` ).
#. Changing the setting in a particular process (*e.g.,* ``[osd.1]`` )

Overriding a global setting affects all child processes, except those that
you specifically override. 

A typical global setting involves activating authentication. For example:

.. code-block:: ini

	[global]
		#Enable authentication between hosts within the cluster.
		#v 0.54 and earlier
		auth supported = cephx
		
		#v 0.55 and after
		auth cluster required = cephx
		auth service required = cephx
		auth client required = cephx


You can specify settings that apply to a particular type of daemon. When you
specify settings under ``[osd]``, ``[mon]`` or ``[mds]`` without specifying a
particular instance, the setting will apply to all OSDs, monitors or metadata
daemons respectively.

You may specify settings for particular instances of a daemon. You may specify
an instance by entering its type, delimited by a period (.) and by the
instance ID. The instance ID for an OSD is always numeric, but it may be
alphanumeric for monitors and metadata servers.

.. code-block:: ini

	[osd.1]
		# settings affect osd.1 only.
		
	[mon.a]	
		# settings affect mon.a only.
		
	[mds.b]
		# settings affect mds.b only.

.. _ceph-metavariables:

Metavariables
=============

Metavariables simplify cluster configuration dramatically. When a metavariable
is set in a configuration value, Ceph expands the metavariable into a concrete
value. Metavariables are very powerful when used within the ``[global]``,
``[osd]``, ``[mon]`` or ``[mds]`` sections of your configuration file. Ceph
metavariables are similar to Bash shell expansion.

Ceph supports the following metavariables: 


``$cluster``

:Description: Expands to the cluster name. Useful when running multiple clusters on the same hardware.
:Example: ``/etc/ceph/$cluster.keyring``
:Default: ``ceph``


``$type``

:Description: Expands to one of ``mds``, ``osd``, or ``mon``, depending on the type of the current daemon.
:Example: ``/var/lib/ceph/$type``


``$id``

:Description: Expands to the daemon identifier. For ``osd.0``, this would be ``0``; for ``mds.a``, it would be ``a``.
:Example: ``/var/lib/ceph/$type/$cluster-$id``


``$host``

:Description: Expands to the host name of the current daemon.


``$name``

:Description: Expands to ``$type.$id``.
:Example: ``/var/run/ceph/$cluster-$name.asok``

.. _ceph-conf-common-settings:

Common Settings
===============

The `Hardware Recommendations`_ section provides some hardware guidelines for
configuring the cluster. It is possible for a single host to run multiple
daemons. For example, a single host with multiple disks or RAIDs may run one
``ceph-osd`` for each disk or RAID. Additionally, a host may run both a
``ceph-mon`` and an ``ceph-osd`` daemon on the same host. Ideally, you will have
a host for a particular type of process. For example, one host may run
``ceph-osd`` daemons, another host may run a ``ceph-mds`` daemon, and other
hosts may run ``ceph-mon`` daemons.

Each host has a name identified by the ``host`` setting. Monitors also  specify
a network address and port (i.e., domain name or IP address) identified by the
``addr`` setting.  A basic configuration file will typically specify only
minimal settings for  each instance of a daemon. For example:

.. code-block:: ini

	[mon.a]
		host = hostName
		mon addr = 150.140.130.120:6789
		
	[osd.0]
		host = hostName

.. important:: The ``host`` setting is the short name of the host (i.e., not 
   an fqdn). It is **NOT** an IP address either.  Enter ``hostname -s`` on 
   the command line to retrieve the name of the host. Also, this setting is 
   **ONLY** for ``mkcephfs`` and manual deployment. It **MUST NOT**
   be used with ``chef`` or ``ceph-deploy``.

.. _Hardware Recommendations: ../../../install/hardware-recommendations

.. _ceph-network-config:

Networks
========

Monitors listen on port 6789 by default, while metadata servers and OSDs listen
on the first available port beginning at 6800. Ensure that you open port 6789 on
hosts that run a monitor daemon, and open one port beginning at port 6800 for
each OSD or metadata server that runs on the host. Ports are host-specific, so
you don't need to open any more ports than the number of daemons running on
that host, other than potentially a few spares. You may consider opening a few
additional ports in case a daemon fails and restarts without letting go of the
port such that the restarted daemon binds to a new port. If you set up separate
public and cluster networks, you may need to make entries for each network.
For example:: 

	iptables -A INPUT -m multiport -p tcp -s {ip-address}/{netmask} --dports 6789,6800:6810 -j ACCEPT


In our `hardware recommendations`_ section, we recommend having at least two NIC
cards, because Ceph can support two networks: a public (front-side) network, and
a cluster (back-side) network. Ceph functions just fine with a public network
only. You only need to specify the public and cluster network settings if you
use both public and cluster networks.

There are several reasons to consider operating two separate networks. First,
OSDs handle data replication for the clients. When OSDs replicate data more than
once, the network load between OSDs easily dwarfs the network load between
clients and the Ceph cluster. This can introduce latency and create a
performance problem. Second, while most people are generally civil, a very tiny
segment of the population likes to engage in what's known as a Denial of Service
(DoS) attack. When traffic between OSDs gets disrupted, placement groups may no
longer reflect an ``active + clean`` state, which may prevent users from reading
and writing data. A great way to defeat this type of attack is to maintain a
completely separate cluster network that doesn't connect directly to the
internet.

To configure the networks, add the following options to the ``[global]`` section
of your Ceph configuration file. 

.. code-block:: ini

	[global]
		public network = {public-network-ip-address/netmask}
		cluster network = {enter cluster-network-ip-address/netmask}
	
To configure Ceph hosts to use the networks, you should set the following options
in the daemon instance sections of your ``ceph.conf`` file. 

.. code-block:: ini

	[osd.0]
		public addr = {host-public-ip-address}
		cluster addr = {host-cluster-ip-address}

.. _hardware recommendations: ../../../install/hardware-recommendations

Authentication
==============

.. versionadded:: 0.55

For Bobtail (v 0.56) and beyond, you should expressly enable or disable authentication
in the ``[global]`` section of your Ceph configuration file. :: 

		auth cluster required = cephx
		auth service required = cephx
		auth client required = cephx

See `Cephx Authentication`_ for additional details.

.. important:: When upgrading, we recommend expressly disabling authentication first, 
   then perform the upgrade. Once the upgrade is complete, re-enable authentication.

.. _Cephx Authentication: ../../operations/authentication

.. _ceph-monitor-config:

Monitors
========

Ceph production clusters typically deploy with a minimum 3 monitors to ensure
high availability should a monitor instance crash. An odd number of monitors (3)
ensures that the Paxos algorithm can determine which version of the cluster map
is the most recent from a quorum of monitors.

.. note:: You may deploy Ceph with a single monitor, but if the instance fails,
	  the lack of a monitor may interrupt data service availability.

Ceph monitors typically listen on port ``6789``. For example:

.. code-block:: ini 

	[mon.a]
		host = hostName
		mon addr = 150.140.130.120:6789

By default, Ceph expects that you will store a monitor's data under the following path:: 

	/var/lib/ceph/mon/$cluster-$id
	
You must create the corresponding directory yourself. With metavariables fully 
expressed and a cluster named "ceph", the foregoing directory would evaluate to:: 

	/var/lib/ceph/mon/ceph-a
	
You may override this path using the ``mon data`` setting. We don't recommend 
changing the default location. Create the default directory on your new monitor host. :: 

	ssh {new-mon-host}
	sudo mkdir /var/lib/ceph/mon/ceph-{mon-letter}


.. _ceph-osd-config:

OSDs
====

Ceph production clusters typically deploy OSDs where one host has one OSD daemon
running a filestore on one data disk. A typical deployment specifies a journal 
size and whether the file store's extended attributes (XATTRs) use an 
object map (i.e., when running on the ``ext4`` filesystem). For example: 

.. code-block:: ini

	[osd]
		osd journal size = 10000
		filestore xattr use omap = true #enables the object map. Only if running ext4.
		
	[osd.0]
		host = {hostname}


By default, Ceph expects that you will store an OSD's data with the following path:: 

	/var/lib/ceph/osd/$cluster-$id
	
You must create the corresponding directory yourself. With metavariables fully 
expressed and a cluster named "ceph", the foregoing directory would evaluate to:: 

	/var/lib/ceph/osd/ceph-0
	
You may override this path using the ``osd data`` setting. We don't recommend 
changing the default location. Create the default directory on your new OSD host. :: 

	ssh {new-osd-host}
	sudo mkdir /var/lib/ceph/osd/ceph-{osd-number}	

The ``osd data`` path ideally leads to a mount point with a hard disk that is
separate from the hard disk storing and running the operating system and
daemons. If the OSD is for a disk other than the OS disk, prepare it for
use with Ceph, and mount it to the directory you just created:: 

	ssh {new-osd-host}
	sudo mkfs -t {fstype} /dev/{disk}
	sudo mount -o user_xattr /dev/{hdd} /var/lib/ceph/osd/ceph-{osd-number}

We recommend using the ``xfs`` file system or the ``btrfs`` file system when 
running :command:mkfs. 

By default, Ceph expects that you will store an OSDs journal with the following path:: 

	/var/lib/ceph/osd/$cluster-$id/journal

Without performance optimization, Ceph stores the journal on the same disk as
the OSDs data. An OSD optimized for performance may use a separate disk to store
journal data (e.g., a solid state drive delivers high performance journaling).

Ceph's default ``osd journal size`` is 0, so you will need to set this in your 
``ceph.conf`` file. A journal size should find the product of the ``filestore
max sync interval`` and the expected throughput, and multiply the product by 
two (2)::  
	  
	osd journal size = {2 * (expected throughput * filestore max sync interval)}

The expected throughput number should include the expected disk throughput
(i.e., sustained data transfer rate), and network throughput. For example, 
a 7200 RPM disk will likely have approximately 100 MB/s. Taking the ``min()``
of the disk and network throughput should provide a reasonable expected 
throughput. Some users just start off with a 10GB journal size. For 
example::

	osd journal size = 10000

.. _ceph-logging-and-debugging:

Logs / Debugging
================

Ceph is still on the leading edge, so you may encounter situations that require
modifying logging output and using Ceph's debugging. To activate Ceph's
debugging output (*i.e.*, ``dout()``), you may add ``debug`` settings to your
configuration. Ceph's logging levels operate on a scale of 1 to 20, where 1 is
terse and 20 is verbose. 

.. note:: See `Debugging and Logging`_ for details on log rotation.

.. _Debugging and Logging: ../../operations/debug

Subsystems common to each daemon may be set under ``[global]`` in your
configuration  file. Subsystems for particular daemons are set under the daemon
section in your configuration file (*e.g.*, ``[mon]``, ``[osd]``, ``[mds]``).
For example:: 

	[global]
		debug ms = 1
		
	[mon]
		debug mon = 20
		debug paxos = 20
		debug auth = 20
		 
 	[osd]
 		debug osd = 20
 		debug filestore = 20
 		debug journal = 20
 		debug monc = 20
 		
	[mds]
		debug mds = 20
		debug mds balancer = 20
		debug mds log = 20
		debug mds migrator = 20

When your system is running well, choose appropriate logging levels and remove 
unnecessary debugging settings to ensure your cluster runs optimally. Logging
debug output messages is relatively slow, and a waste of resources when operating
your cluster. 

.. tip: When debug output slows down your system, the latency can hide race conditions.

Each subsystem has a logging level for its output logs,  and for its logs
in-memory. You may set different values for each of these subsystems by setting
a log file level and a memory  level for debug logging. For example:: 

	debug {subsystem} {log-level}/{memory-level}
	#for example
	debug mds log 1/20

+--------------------+-----------+--------------+
| Subsystem          | Log Level | Memory Level |
+====================+===========+==============+
| ``default``        |     0     |      5       |
+--------------------+-----------+--------------+
| ``lockdep``        |     0     |      5       |
+--------------------+-----------+--------------+
| ``context``        |     0     |      5       |
+--------------------+-----------+--------------+
| ``crush``          |     1     |      5       |
+--------------------+-----------+--------------+
| ``mds``            |     1     |      5       |
+--------------------+-----------+--------------+
| ``mds balancer``   |     1     |      5       |
+--------------------+-----------+--------------+
| ``mds locker``     |     1     |      5       |
+--------------------+-----------+--------------+
| ``mds log``        |     1     |      5       |
+--------------------+-----------+--------------+
| ``mds log expire`` |     1     |      5       |
+--------------------+-----------+--------------+
| ``mds migrator``   |     1     |      5       |
+--------------------+-----------+--------------+
| ``buffer``         |     0     |      0       |
+--------------------+-----------+--------------+
| ``timer``          |     0     |      5       |
+--------------------+-----------+--------------+
| ``filer``          |     0     |      5       |
+--------------------+-----------+--------------+
| ``objecter``       |     0     |      0       |
+--------------------+-----------+--------------+
| ``rados``          |     0     |      5       |
+--------------------+-----------+--------------+
| ``rbd``            |     0     |      5       |
+--------------------+-----------+--------------+
| ``journaler``      |     0     |      5       |
+--------------------+-----------+--------------+
| ``objectcacher``   |     0     |      5       |
+--------------------+-----------+--------------+
| ``client``         |     0     |      5       |
+--------------------+-----------+--------------+
| ``osd``            |     0     |      5       |
+--------------------+-----------+--------------+
| ``optracker``      |     0     |      5       |
+--------------------+-----------+--------------+
| ``objclass``       |     0     |      5       |
+--------------------+-----------+--------------+
| ``filestore``      |     1     |      5       |
+--------------------+-----------+--------------+
| ``journal``        |     1     |      5       |
+--------------------+-----------+--------------+
| ``ms``             |     0     |      5       |
+--------------------+-----------+--------------+
| ``mon``            |     1     |      5       |
+--------------------+-----------+--------------+
| ``monc``           |     0     |      5       |
+--------------------+-----------+--------------+
| ``paxos``          |     0     |      5       |
+--------------------+-----------+--------------+
| ``tp``             |     0     |      5       |
+--------------------+-----------+--------------+
| ``auth``           |     1     |      5       |
+--------------------+-----------+--------------+
| ``finisher``       |     1     |      5       |
+--------------------+-----------+--------------+
| ``heartbeatmap``   |     1     |      5       |
+--------------------+-----------+--------------+
| ``perfcounter``    |     1     |      5       |
+--------------------+-----------+--------------+
| ``rgw``            |     1     |      5       |
+--------------------+-----------+--------------+
| ``hadoop``         |     1     |      5       |
+--------------------+-----------+--------------+
| ``asok``           |     1     |      5       |
+--------------------+-----------+--------------+
| ``throttle``       |     1     |      5       |
+--------------------+-----------+--------------+


Example ceph.conf
=================

.. literalinclude:: demo-ceph.conf
   :language: ini

.. _ceph-runtime-config:

Runtime Changes
===============

Ceph allows you to make changes to the configuration of an ``ceph-osd``,
``ceph-mon``, or ``ceph-mds`` daemon at runtime. This capability is quite
useful for increasing/decreasing logging output, enabling/disabling debug
settings, and even for runtime optimization. The following reflects runtime
configuration usage::

	ceph {daemon-type} tell {id or *} injectargs '--{name} {value} [--{name} {value}]'
	
Replace ``{daemon-type}`` with one of ``osd``, ``mon`` or ``mds``. You may apply
the  runtime setting to all daemons of a particular type with ``*``, or specify
a specific  daemon's ID (i.e., its number or letter). For example, to increase
debug logging for a ``ceph-osd`` daemon named ``osd.0``, execute the following:: 

	ceph osd tell 0 injectargs '--debug-osd 20 --debug-ms 1'

In your ``ceph.conf`` file, you may use spaces when specifying a
setting name.  When specifying a setting name on the command line,
ensure that you use an underscore or hyphen (``_`` or ``-``) between
terms (e.g., ``debug osd`` becomes ``debug-osd``).


Viewing a Configuration at Runtime
==================================

If your Ceph cluster is running, and you would like to see the configuration
settings from a running daemon, execute the following:: 

	ceph --admin-daemon {/path/to/admin/socket} config show | less
	
The default path for the admin socket for each daemon is:: 

	/var/run/ceph/$cluster-$name.asok
	
At real time, the metavariables will evaluate to the actual cluster name
and daemon name. For example, if the cluster name is ``ceph`` (it is by default)
and you want to retrieve the configuration for ``osd.0``, use the following::

	ceph --admin-daemon /var/run/ceph/ceph-osd.0.asok config show | less


Running Multiple Clusters
=========================

With Ceph, you can run multiple clusters on the same hardware. Running multiple
clusters provides a higher level of isolation compared to using different pools
on the same cluster with different CRUSH rulesets. A separate cluster will have
separate monitor, OSD and metadata server processes. When running Ceph with 
default settings, the default cluster name is ``ceph``, which means you would 
save your Ceph configuration file with the file name ``ceph.conf`` in the 
``/etc/ceph`` default directory. 

When you run multiple clusters, you must name your cluster and save the Ceph
configuration file with the name of the cluster. For example, a cluster named
``openstack`` will have a Ceph configuration file with the file name
``openstack.conf`` in the  ``/etc/ceph`` default directory. 

.. important:: Cluster names must consist of letters a-z and digits 0-9 only.

Separate clusters imply separate data disks and journals, which are not shared
between clusters. Referring to `Metavariables`_, the ``$cluster``  metavariable
evaluates to the cluster name (i.e., ``openstack`` in the  foregoing example).
Various settings use the ``$cluster`` metavariable, including: 

- ``keyring``
- ``admin socket``
- ``log file``
- ``pid file``
- ``mon data``
- ``mon cluster log file``
- ``osd data``
- ``osd journal``
- ``mds data``
- ``rgw data``

See `General Settings`_, `OSD Settings`_, `Monitor Settings`_, `MDS Settings`_, 
`RGW Settings`_ and `Log Settings`_ for relevant path defaults that use the 
``$cluster`` metavariable.

.. _General Settings: ../general-config-ref
.. _OSD Settings: ../osd-config-ref
.. _Monitor Settings: ../mon-config-ref
.. _MDS Settings: ../../../cephfs/mds-config-ref
.. _RGW Settings: ../../../radosgw/config-ref/
.. _Log Settings: ../log-and-debug-ref

When deploying the Ceph configuration file, ensure that you use the cluster name
in your command line syntax. For example:: 

	ssh myserver01 sudo tee /etc/ceph/openstack.conf < /etc/ceph/openstack.conf

When creating default directories or files, you should also use the cluster
name at the appropriate places in the path. For example:: 

	sudo mkdir /var/lib/ceph/osd/openstack-0
	sudo mkdir /var/lib/ceph/mon/openstack-a
	
.. important:: When running monitors on the same host, you should use 
   different ports. By default, monitors use port 6789. If you already 
   have monitors using port 6789, use a different port for your other cluster(s). 

To invoke a cluster other than the default ``ceph`` cluster, use the 
``--cluster=clustername`` option with the ``ceph`` command. For example:: 

	ceph --cluster=openstack health
