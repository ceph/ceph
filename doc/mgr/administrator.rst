.. _mgr-administrator-guide:

ceph-mgr administrator's guide
==============================

Manual setup
------------

Usually, you would set up a ceph-mgr daemon using a tool such
as ceph-ansible.  These instructions describe how to set up
a ceph-mgr daemon manually.

First, create an authentication key for your daemon::

    ceph auth get-or-create mgr.$name mon 'allow profile mgr' osd 'allow *' mds 'allow *'

Place that key as file named ``keyring`` into ``mgr data`` path, which for a cluster "ceph"
and mgr $name "foo" would be ``/var/lib/ceph/mgr/ceph-foo`` respective ``/var/lib/ceph/mgr/ceph-foo/keyring``.

Start the ceph-mgr daemon::

    ceph-mgr -i $name

Check that the mgr has come up by looking at the output
of ``ceph status``, which should now include a mgr status line::

    mgr active: $name

Client authentication
---------------------

The manager is a new daemon which requires new CephX capabilities. If you upgrade
a cluster from an old version of Ceph, or use the default install/deploy tools,
your admin client should get this capability automatically. If you use tooling from
elsewhere, you may get EACCES errors when invoking certain ceph cluster commands.
To fix that, add a "mgr allow \*" stanza to your client's cephx capabilities by
`Modifying User Capabilities`_.

High availability
-----------------

In general, you should set up a ceph-mgr on each of the hosts
running a ceph-mon daemon to achieve the same level of availability.

By default, whichever ceph-mgr instance comes up first will be made
active by the monitors, and the others will be standbys.  There is
no requirement for quorum among the ceph-mgr daemons.

If the active daemon fails to send a beacon to the monitors for
more than :confval:`mon_mgr_beacon_grace`, then it will be replaced
by a standby.

If you want to pre-empt failover, you can explicitly mark a ceph-mgr
daemon as failed using ``ceph mgr fail <mgr name>``.

Using modules
-------------

Use the command ``ceph mgr module ls`` to see which modules are
available, and which are currently enabled.  Enable or disable modules
using the commands ``ceph mgr module enable <module>`` and
``ceph mgr module disable <module>`` respectively.

If a module is *enabled* then the active ceph-mgr daemon will load
and execute it.  In the case of modules that provide a service,
such as an HTTP server, the module may publish its address when it
is loaded.  To see the addresses of such modules, use the command 
``ceph mgr services``.

Some modules may also implement a special standby mode which runs on
standby ceph-mgr daemons as well as the active daemon.  This enables
modules that provide services to redirect their clients to the active
daemon, if the client tries to connect to a standby.

Consult the documentation pages for individual manager modules for more
information about what functionality each module provides.

Here is an example of enabling the :term:`Dashboard` module:

.. code-block:: console

	$ ceph mgr module ls
	{
		"enabled_modules": [
			"restful",
			"status"
		],
		"disabled_modules": [
			"dashboard"
		]
	}

	$ ceph mgr module enable dashboard
	$ ceph mgr module ls
	{
		"enabled_modules": [
			"restful",
			"status",
			"dashboard"
		],
		"disabled_modules": [
		]
	}

	$ ceph mgr services
	{
		"dashboard": "http://myserver.com:7789/",
		"restful": "https://myserver.com:8789/"
	}


The first time the cluster starts, it uses the :confval:`mgr_initial_modules`
setting to override which modules to enable.  However, this setting
is ignored through the rest of the lifetime of the cluster: only
use it for bootstrapping.  For example, before starting your
monitor daemons for the first time, you might add a section like
this to your ``ceph.conf``:

.. code-block:: ini

    [mon]
        mgr_initial_modules = dashboard balancer

Module Pool
-----------

The manager creates a pool for use by its module to store state. The name of
this pool is ``.mgr`` (with the leading ``.`` indicating a reserved pool
name).

.. note::

   Prior to Quincy, the ``devicehealth`` module created a
   ``device_health_metrics`` pool to store device SMART statistics. With
   Quincy, this pool is automatically renamed to be the common manager module
   pool.


Calling module commands
-----------------------

Where a module implements command line hooks, the commands will
be accessible as ordinary Ceph commands.  Ceph will automatically incorporate
module commands into the standard CLI interface and route them appropriately to
the module.::

    ceph <command | help>

Configuration
-------------

.. confval:: mgr_module_path
.. confval:: mgr_initial_modules
.. confval:: mgr_data
.. confval:: mgr_tick_period
.. confval:: mon_mgr_beacon_grace

.. _Modifying User Capabilities: ../../rados/operations/user-management/#modify-user-capabilities
