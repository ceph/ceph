==========================
 Ceph Configuration Files
==========================
When you start the Ceph service, the initialization process activates a series
of daemons that run in the background. The hosts in a typical RADOS cluster run
at least one of three processes or daemons:

- RADOS (``ceph-osd``)
- Monitor (``ceph-mon``)
- Metadata Server (``ceph-mds``)

Each process or daemon looks for a ``ceph.conf`` file that provides its
configuration settings. The default ``ceph.conf`` locations in sequential
order include:

#. ``$CEPH_CONF`` (*i.e.,* the path following the ``$CEPH_CONF`` environment variable)
#. ``-c path/path``  (*i.e.,* the ``-c`` command line argument)
#. ``/etc/ceph/ceph.conf``
#. ``~/.ceph/config``
#. ``./ceph.conf`` (*i.e.,* in the current working directory)


The ``ceph.conf`` file provides the settings for each Ceph daemon. Once you
have installed the Ceph packages on the OSD Cluster hosts, you need to create
a ``ceph.conf`` file to configure your OSD cluster.

Creating ``ceph.conf``
----------------------
The ``ceph.conf`` file defines:

- Cluster Membership
- Host Names
- Paths to Hosts
- Runtime Options

You can add comments to the ``ceph.conf`` file by preceding comments with
a semi-colon (;). For example::

	; <--A semi-colon precedes a comment
	; A comment may be anything, and always follows a semi-colon on each line.
	; We recommend that you provide comments in your configuration file(s).

Configuration File Basics
~~~~~~~~~~~~~~~~~~~~~~~~~
The ``ceph.conf`` file configures each instance of the three common processes
in a RADOS cluster.

+-----------------+--------------+--------------+-----------------+-------------------------------------------------+
| Setting Scope   | Process      | Setting      | Instance Naming | Description                                     |
+=================+==============+==============+=================+=================================================+
| All Modules     |     All      | ``[global]`` | N/A             | Settings affect all instances of all daemons.   |
+-----------------+--------------+--------------+-----------------+-------------------------------------------------+
| RADOS           | ``ceph-osd`` |  ``[osd]``   | Numeric         | Settings affect RADOS instances only.           |
+-----------------+--------------+--------------+-----------------+-------------------------------------------------+
| Monitor         | ``ceph-mon`` |  ``[mon]``   | Alphanumeric    | Settings affect monitor instances only.         |
+-----------------+--------------+--------------+-----------------+-------------------------------------------------+
| Metadata Server | ``ceph-mds`` |  ``[mds]``   | Alphanumeric    | Settings affect MDS instances only.             |
+-----------------+--------------+--------------+-----------------+-------------------------------------------------+



Metavariables
~~~~~~~~~~~~~
The configuration system supports certain 'metavariables,' which are typically
used in ``[global]`` or process/daemon settings. If metavariables occur inside
a configuration value, Ceph expands them into a concrete value--similar to how
Bash shell expansion works.

There are a few different metavariables:

+--------------+----------------------------------------------------------------------------------------------------------+
| Metavariable | Description                                                                                              |
+==============+==========================================================================================================+
| ``$host``    | Expands to the host name of the current daemon.                                                          |
+--------------+----------------------------------------------------------------------------------------------------------+
| ``$type``    | Expands to one of ``mds``, ``osd``, or ``mon``, depending on the type of the current daemon.             |
+--------------+----------------------------------------------------------------------------------------------------------+
|  ``$id``     | Expands to the daemon identifier. For ``osd.0``, this would be ``0``; for ``mds.a``, it would be ``a``.  |
+--------------+----------------------------------------------------------------------------------------------------------+
|  ``$num``    | Same as ``$id``.                                                                                         |
+--------------+----------------------------------------------------------------------------------------------------------+
| ``$name``    | Expands to ``$type.$id``.                                                                                |
+--------------+----------------------------------------------------------------------------------------------------------+
| ``$cluster`` | Expands to the cluster name. Useful when running multiple clusters on the same hardware.                 |
+--------------+----------------------------------------------------------------------------------------------------------+

Global Settings
~~~~~~~~~~~~~~~
The Ceph configuration file supports a hierarchy of settings, where child
settings inherit the settings of the parent. Global settings affect all
instances of all processes in the cluster. Use the ``[global]`` setting for
values that are common for all hosts in the cluster. You can override each
``[global]`` setting by:

#. Changing the setting in a particular process type (*e.g.,* ``[osd]``, ``[mon]``, ``[mds]`` ).
#. Changing the setting in a particular process (*e.g.,* ``[osd.1]`` )

Overriding a global setting affects all child processes, except those that
you specifically override. For example::

	[global]
		; Enable authentication between hosts within the cluster.
		auth supported = cephx

Process/Daemon Settings
~~~~~~~~~~~~~~~~~~~~~~~
You can specify settings that apply to a particular type of process. When you
specify settings under ``[osd]``, ``[mon]`` or ``[mds]`` without specifying a
particular instance, the setting will apply to all OSDs, monitors or metadata
daemons respectively.

For details on settings for each type of daemon, 
see the following sections. 

.. toctree::

	OSD Settings <osd-config-ref>
	Monitor Settings <mon-config-ref>
	Metadata Server Settings <mds-config-ref>	

Instance Settings
~~~~~~~~~~~~~~~~~
You may specify settings for particular instances of an daemon. You may specify
an instance by entering its type, delimited by a period (.) and by the
instance ID. The instance ID for an OSD is always numeric, but it may be
alphanumeric for monitors and metadata servers. ::

	[osd.1]
		; settings affect osd.1 only.
	[mon.a1]
		; settings affect mon.a1 only.
	[mds.b2]
		; settings affect mds.b2 only.
		

``host`` and ``addr`` Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The `Hardware Recommendations <../../install/hardware-recommendations>`_ section
provides some hardware guidelines for configuring the cluster. It is possible
for a single host to run multiple daemons. For example, a single host with
multiple disks or RAIDs may run one ``ceph-osd`` for each disk or RAID.
Additionally, a host may run both a ``ceph-mon`` and an ``ceph-osd`` daemon
on the same host. Ideally, you will have a host for a particular type of
process. For example, one host may run ``ceph-osd`` daemons, another host
may run a ``ceph-mds`` daemon, and other hosts may run ``ceph-mon`` daemons.

Each host has a name identified by the ``host`` setting, and a network location
(i.e., domain name or IP address) identified by the ``addr`` setting. For example::

	[mon.a]
		host = hostName
		mon addr = 150.140.130.120:6789
	[osd.0]
		host = hostName

Monitor Configuration
~~~~~~~~~~~~~~~~~~~~~
Ceph typically deploys with 3 monitors to ensure high availability should a
monitor instance crash. An odd number of monitors (3) ensures that the Paxos
algorithm can determine which version of the cluster map is the most accurate.

.. note:: You may deploy Ceph with a single monitor, but if the instance fails,
	  the lack of a monitor may interrupt data service availability.

Ceph monitors typically listen on port ``6789``. For example:: 

	[mon.a]
		host = hostName
		mon addr = 150.140.130.120:6789

Example Configuration File
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: demo-ceph.conf
   :language: ini


``iptables`` Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~
Monitors listen on port 6789, while metadata servers and OSDs listen on the first
available port beginning at 6800. Ensure that you open port 6789 on hosts that run
a monitor daemon, and open one port beginning at port 6800 for each OSD or metadata
server that runs on the host. For example:: 

	iptables -A INPUT -m multiport -p tcp -s 192.168.1.0/24 --dports 6789,6800:6803 -j ACCEPT