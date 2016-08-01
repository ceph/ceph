==================
 Configuring Ceph
==================

When you start the Ceph service, the initialization process activates a series
of daemons that run in the background. A :term:`Ceph Storage Cluster` runs 
two types of daemons: 

- :term:`Ceph Monitor` (``ceph-mon``)
- :term:`Ceph OSD Daemon` (``ceph-osd``)

Ceph Storage Clusters that support the :term:`Ceph Filesystem` run at least one
:term:`Ceph Metadata Server` (``ceph-mds``). Clusters that support :term:`Ceph
Object Storage` run Ceph Gateway daemons (``radosgw``). For your convenience,
each daemon has a series of default values (*i.e.*, many are set by
``ceph/src/common/config_opts.h``). You may override these settings with a Ceph
configuration file.


.. _ceph-conf-file:

The Configuration File
======================

When you start a Ceph Storage Cluster, each daemon looks for a Ceph
configuration file (i.e., ``ceph.conf`` by default) that provides the cluster's
configuration settings. For manual deployments, you need to create a Ceph
configuration file. For tools that create configuration files for you (*e.g.*,
``ceph-deploy``, Chef, etc.), you may use the information contained herein as a
reference. The Ceph configuration file defines:

- Cluster Identity
- Authentication settings
- Cluster membership
- Host names
- Host addresses
- Paths to keyrings
- Paths to journals
- Paths to data
- Other runtime options

The default Ceph configuration file locations in sequential order include:

#. ``$CEPH_CONF`` (*i.e.,* the path following the ``$CEPH_CONF`` 
   environment variable)
#. ``-c path/path``  (*i.e.,* the ``-c`` command line argument)
#. ``/etc/ceph/ceph.conf``
#. ``~/.ceph/config``
#. ``./ceph.conf`` (*i.e.,* in the current working directory)


The Ceph configuration file uses an *ini* style syntax. You can add comments 
by preceding comments with a pound sign (#) or a semi-colon (;).  For example:

.. code-block:: ini

	# <--A number (#) sign precedes a comment.
	; A comment may be anything. 
	# Comments always follow a semi-colon (;) or a pound (#) on each line.
	# The end of the line terminates a comment.
	# We recommend that you provide comments in your configuration file(s).


.. _ceph-conf-settings:

Config Sections 
===============

The configuration file can configure all Ceph daemons in a Ceph Storage Cluster,
or all Ceph daemons of a particular type. To configure a series of daemons, the
settings must be included under the processes that will receive the
configuration as follows: 

``[global]``

:Description: Settings under ``[global]`` affect all daemons in a Ceph Storage
              Cluster.
              
:Example: ``auth supported = cephx``

``[osd]``

:Description: Settings under ``[osd]`` affect all ``ceph-osd`` daemons in 
              the Ceph Storage Cluster, and override the same setting in 
              ``[global]``.

:Example: ``osd journal size = 1000``

``[mon]``

:Description: Settings under ``[mon]`` affect all ``ceph-mon`` daemons in 
              the Ceph Storage Cluster, and override the same setting in 
              ``[global]``.

:Example: ``mon addr = 10.0.0.101:6789``


``[mds]``

:Description: Settings under ``[mds]`` affect all ``ceph-mds`` daemons in 
              the Ceph Storage Cluster, and override the same setting in 
              ``[global]``. 

:Example: ``host = myserver01``

``[client]``

:Description: Settings under ``[client]`` affect all Ceph Clients 
              (e.g., mounted Ceph Filesystems, mounted Ceph Block Devices, 
              etc.).

:Example: ``log file = /var/log/ceph/radosgw.log``


Global settings affect all instances of all daemon in the Ceph Storage Cluster.
Use the ``[global]`` setting for values that are common for all daemons in the
Ceph Storage Cluster. You can override each ``[global]`` setting by:

#. Changing the setting in a particular process type 
   (*e.g.,* ``[osd]``, ``[mon]``, ``[mds]`` ).

#. Changing the setting in a particular process (*e.g.,* ``[osd.1]`` ).

Overriding a global setting affects all child processes, except those that
you specifically override in a particular daemon. 

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

A typical daemon-wide setting involves setting journal sizes, filestore
settings, etc. For example:

.. code-block:: ini

	[osd]
	osd journal size = 1000


You may specify settings for particular instances of a daemon. You may specify
an instance by entering its type, delimited by a period (.) and by the instance
ID. The instance ID for a Ceph OSD Daemon is always numeric, but it may be
alphanumeric for Ceph Monitors and Ceph Metadata Servers.

.. code-block:: ini

	[osd.1]
	# settings affect osd.1 only.
		
	[mon.a]	
	# settings affect mon.a only.
		
	[mds.b]
	# settings affect mds.b only.


If the daemon you specify is a Ceph Gateway client, specify the daemon and the 
instance, delimited by a period (.). For example:: 

	[client.radosgw.instance-name]
	# settings affect client.radosgw.instance-name only.



.. _ceph-metavariables:

Metavariables
=============

Metavariables simplify Ceph Storage Cluster configuration dramatically. When a
metavariable is set in a configuration value, Ceph expands the metavariable into
a concrete value. Metavariables are very powerful when used within the
``[global]``, ``[osd]``, ``[mon]``, ``[mds]`` or ``[client]`` sections of your 
configuration file. Ceph metavariables are similar to Bash shell expansion.

Ceph supports the following metavariables: 


``$cluster``

:Description: Expands to the Ceph Storage Cluster name. Useful when running 
              multiple Ceph Storage Clusters on the same hardware.

:Example: ``/etc/ceph/$cluster.keyring``
:Default: ``ceph``


``$type``

:Description: Expands to one of ``mds``, ``osd``, or ``mon``, depending on the 
              type of the instant daemon.

:Example: ``/var/lib/ceph/$type``


``$id``

:Description: Expands to the daemon identifier. For ``osd.0``, this would be 
              ``0``; for ``mds.a``, it would be ``a``.

:Example: ``/var/lib/ceph/$type/$cluster-$id``


``$host``

:Description: Expands to the host name of the instant daemon.


``$name``

:Description: Expands to ``$type.$id``.
:Example: ``/var/run/ceph/$cluster-$name.asok``


.. _ceph-conf-common-settings:

Common Settings
===============

The `Hardware Recommendations`_ section provides some hardware guidelines for
configuring a Ceph Storage Cluster. It is possible for a single :term:`Ceph
Node` to run multiple daemons. For example, a single node with multiple drives
may run one ``ceph-osd`` for each drive. Ideally, you will  have a node for a
particular type of process. For example, some nodes may run ``ceph-osd``
daemons, other nodes may run ``ceph-mds`` daemons, and still  other nodes may
run ``ceph-mon`` daemons.

Each node has a name identified by the ``host`` setting. Monitors also specify
a network address and port (i.e., domain name or IP address) identified by the
``addr`` setting.  A basic configuration file will typically specify only
minimal settings for each instance of monitor daemons. For example:

.. code-block:: ini

	[global]
	mon_initial_members = ceph1
	mon_host = 10.0.0.1


.. important:: The ``host`` setting is the short name of the node (i.e., not 
   an fqdn). It is **NOT** an IP address either.  Enter ``hostname -s`` on 
   the command line to retrieve the name of the node. Do not use ``host`` 
   settings for anything other than initial monitors unless you are deploying
   Ceph manually. You **MUST NOT** specify ``host`` under individual daemons 
   when using deployment tools like ``chef`` or ``ceph-deploy``, as those tools 
   will enter the appropriate values for you in the cluster map.


.. _ceph-network-config:

Networks
========

See the `Network Configuration Reference`_ for a detailed discussion about
configuring a network for use with Ceph.


Monitors
========

Ceph production clusters typically deploy with a minimum 3 :term:`Ceph Monitor`
daemons to ensure high availability should a monitor instance crash. At least
three (3) monitors ensures that the Paxos algorithm can determine which version
of the :term:`Ceph Cluster Map` is the most recent from a majority of Ceph
Monitors in the quorum.

.. note:: You may deploy Ceph with a single monitor, but if the instance fails,
	       the lack of other monitors may interrupt data service availability.

Ceph Monitors typically listen on port ``6789``. For example:

.. code-block:: ini 

	[mon.a]
	host = hostName
	mon addr = 150.140.130.120:6789

By default, Ceph expects that you will store a monitor's data under the
following path::

	/var/lib/ceph/mon/$cluster-$id
	
You or a deployment tool (e.g., ``ceph-deploy``) must create the corresponding
directory. With metavariables fully  expressed and a cluster named "ceph", the
foregoing directory would evaluate to:: 

	/var/lib/ceph/mon/ceph-a
	
For additional details, see the `Monitor Config Reference`_.

.. _Monitor Config Reference: ../mon-config-ref


.. _ceph-osd-config:


Authentication
==============

.. versionadded:: Bobtail 0.56

For Bobtail (v 0.56) and beyond, you should expressly enable or disable
authentication in the ``[global]`` section of your Ceph configuration file. ::

	auth cluster required = cephx
	auth service required = cephx
	auth client required = cephx

Additionally, you should enable message signing. See `Cephx Config Reference`_
and  `Cephx Authentication`_ for details. 

.. important:: When upgrading, we recommend expressly disabling authentication 
   first, then perform the upgrade. Once the upgrade is complete, re-enable 
   authentication.

.. _Cephx Authentication: ../../operations/authentication
.. _Cephx Config Reference: ../auth-config-ref


.. _ceph-monitor-config:


OSDs
====

Ceph production clusters typically deploy :term:`Ceph OSD Daemons` where one node
has one OSD daemon running a filestore on one storage drive. A typical
deployment specifies a journal size. For example:

.. code-block:: ini

	[osd]
	osd journal size = 10000
		
	[osd.0]
	host = {hostname} #manual deployments only.


By default, Ceph expects that you will store a Ceph OSD Daemon's data with the 
following path:: 

	/var/lib/ceph/osd/$cluster-$id
	
You or a deployment tool (e.g., ``ceph-deploy``) must create the corresponding
directory. With metavariables fully  expressed and a cluster named "ceph", the
foregoing directory would evaluate to:: 

	/var/lib/ceph/osd/ceph-0
	
You may override this path using the ``osd data`` setting. We don't recommend 
changing the default location. Create the default directory on your OSD host.

:: 

	ssh {osd-host}
	sudo mkdir /var/lib/ceph/osd/ceph-{osd-number}	

The ``osd data`` path ideally leads to a mount point with a hard disk that is
separate from the hard disk storing and running the operating system and
daemons. If the OSD is for a disk other than the OS disk, prepare it for
use with Ceph, and mount it to the directory you just created:: 

	ssh {new-osd-host}
	sudo mkfs -t {fstype} /dev/{disk}
	sudo mount -o user_xattr /dev/{hdd} /var/lib/ceph/osd/ceph-{osd-number}

We recommend using the ``xfs`` file system or the ``btrfs`` file system when
running :command:`mkfs`.

See the `OSD Config Reference`_ for additional configuration details.


Heartbeats
==========

During runtime operations, Ceph OSD Daemons check up on other Ceph OSD Daemons
and report their  findings to the Ceph Monitor. You do not have to provide any
settings. However, if you have network latency issues, you may wish to modify
the settings. 

See `Configuring Monitor/OSD Interaction`_ for additional details.


.. _ceph-logging-and-debugging:

Logs / Debugging
================

Sometimes you may encounter issues with Ceph that require
modifying logging output and using Ceph's debugging. See `Debugging and
Logging`_ for details on log rotation.

.. _Debugging and Logging: ../../troubleshooting/log-and-debug


Example ceph.conf
=================

.. literalinclude:: demo-ceph.conf
   :language: ini

.. _ceph-runtime-config:

Runtime Changes
===============

Ceph allows you to make changes to the configuration of a ``ceph-osd``,
``ceph-mon``, or ``ceph-mds`` daemon at runtime. This capability is quite
useful for increasing/decreasing logging output, enabling/disabling debug
settings, and even for runtime optimization. The following reflects runtime
configuration usage::

	ceph tell {daemon-type}.{id or *} injectargs --{name} {value} [--{name} {value}]
	
Replace ``{daemon-type}`` with one of ``osd``, ``mon`` or ``mds``. You may apply
the  runtime setting to all daemons of a particular type with ``*``, or specify
a specific  daemon's ID (i.e., its number or letter). For example, to increase
debug logging for a ``ceph-osd`` daemon named ``osd.0``, execute the following::

	ceph tell osd.0 injectargs --debug-osd 20 --debug-ms 1

In your ``ceph.conf`` file, you may use spaces when specifying a
setting name.  When specifying a setting name on the command line,
ensure that you use an underscore or hyphen (``_`` or ``-``) between
terms (e.g., ``debug osd`` becomes ``--debug-osd``).


Viewing a Configuration at Runtime
==================================

If your Ceph Storage Cluster is running, and you would like to see the
configuration settings from a running daemon, execute the following:: 

	ceph daemon {daemon-type}.{id} config show | less

If you are on a machine where osd.0 is running, the command would be::

    ceph daemon osd.0 config show | less


Running Multiple Clusters
=========================

With Ceph, you can run multiple Ceph Storage Clusters on the same hardware.
Running multiple clusters provides a higher level of isolation compared to 
using different pools on the same cluster with different CRUSH rulesets. A 
separate cluster will have separate monitor, OSD and metadata server processes. 
When running Ceph with  default settings, the default cluster name is ``ceph``, 
which means you would  save your Ceph configuration file with the file name
``ceph.conf`` in the  ``/etc/ceph`` default directory.

See `ceph-deploy new`_ for details.
.. _ceph-deploy new:../ceph-deploy-new

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
.. _Log Settings: ../../troubleshooting/log-and-debug


When creating default directories or files, you should use the cluster
name at the appropriate places in the path. For example:: 

	sudo mkdir /var/lib/ceph/osd/openstack-0
	sudo mkdir /var/lib/ceph/mon/openstack-a
	
.. important:: When running monitors on the same host, you should use 
   different ports. By default, monitors use port 6789. If you already 
   have monitors using port 6789, use a different port for your other cluster(s). 

To invoke a cluster other than the default ``ceph`` cluster, use the 
``-c {filename}.conf`` option with the ``ceph`` command. For example:: 

	ceph -c {cluster-name}.conf health
	ceph -c openstack.conf health


.. _Hardware Recommendations: ../../../install/hardware-recommendations
.. _hardware recommendations: ../../../install/hardware-recommendations
.. _Network Configuration Reference: ../network-config-ref
.. _OSD Config Reference: ../osd-config-ref
.. _Configuring Monitor/OSD Interaction: ../mon-osd-interaction
.. _ceph-deploy new: ../../deployment/ceph-deploy-new#naming-a-cluster
