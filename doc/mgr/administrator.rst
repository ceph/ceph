.. _mgr-administrator-guide:

ceph-mgr administrator's guide
==============================

Manual setup
------------

Usually, you would set up a ceph-mgr daemon using a tool such
as ceph-ansible.  These instructions describe how to set up
a ceph-mgr daemon manually.

First, create an authentication key for your daemon:

.. prompt:: bash #

   ``ceph auth get-or-create mgr.$name mon 'allow profile mgr' osd 'allow *' mds 'allow *'``

Place that key as file named ``keyring`` into ``mgr data`` path, which for a
cluster "ceph" and mgr $name "foo" would be ``/var/lib/ceph/mgr/ceph-foo``
respective ``/var/lib/ceph/mgr/ceph-foo/keyring``.

Start the ceph-mgr daemon:

.. prompt:: bash #

   ceph-mgr -i $name

Check that the mgr has come up by looking at the output of ``ceph status``,
which should now include a mgr status line::

    mgr active: $name

Interpreting Ceph-Mgr Statuses
==============================

A cluster's health status will show each ``ceph-mgr`` daemon in one of three states:

1. **active**

   This mgr daemon has been fully initialized, which means it is ready to receive
   and execute commands. Only one mgr will be in this state at a time.

2. **active (starting)**

   This mgr daemon has been chosen to be ``active``, but it is not done initializing.
   Although it is not yet ready to execute commands, an operator may still issue commands,
   which will be held and executed once the manager becomes ``active``. Only one mgr will
   be in this state at a time.

3. **standby**

   This mgr daemon is not currently receiving or executing commands, but it is there to
   take over if the current active mgr becomes unavailable. An operator may also manually
   promote standby manager to active via ``ceph mgr fail`` if desired. All other mgr daemons
   which are not ``active`` or ``active (starting)`` will be in this state.

Each of these states are visible in the output of the ``ceph -s``. For example:

.. code-block:: console

   $ ceph -s
     cluster:
       id:     b150f540-745a-460c-a566-376b28b95ac3
       health: HEALTH_OK

     services:
       mon: 3 daemons, quorum a,b,c (age 47m) [leader: a]
       mgr: x(active, starting, since 3s)
       mds: 1/1 daemons up, 2 standby
       osd: 4 osds: 4 up (since 47m), 4 in (since 47m)

     data:
       volumes: 1/1 healthy
       pools:   4 pools, 177 pgs
       objects: 24 objects, 451 KiB
       usage:   4.0 GiB used, 400 GiB / 404 GiB avail
       pgs:     177 active+clean

Client authentication
---------------------

The manager is a new daemon which requires new CephX capabilities. If you
upgrade a cluster from an old version of Ceph, or use the default
install/deploy tools, your admin client should get this capability
automatically. If you use tooling from elsewhere, you may get EACCES errors
when invoking certain ceph cluster commands.  To fix that, add a ``mgr allow
\*`` stanza to your client's cephx capabilities by `Modifying User
Capabilities`_.

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

If you want to preempt failover, you can explicitly mark a ceph-mgr
daemon as failed using ``ceph mgr fail <mgr name>``.

Performance and Scalability
---------------------------

All the mgr modules share a cache that can be enabled with
``ceph config set mgr mgr_ttl_cache_expire_seconds <seconds>``, where seconds
is the time to live of the cached python objects.

It is recommended to enable the cache with a 10 seconds TTL when there are 500+
osds or 10k+ pgs as internal structures might increase in size, and cause latency
issues when requesting large structures. As an example, an OSDMap with 1000 osds
has a approximate size of 4MiB. With heavy load, on a 3000 osd cluster there has
been a 1.5x improvement enabling the cache.

Furthermore, you can run ``ceph daemon mgr.${MGRNAME} perf dump`` to retrieve
perf counters of a mgr module. In ``mgr.cache_hit`` and ``mgr.cache_miss``
you'll find the hit/miss ratio of the mgr cache.

Using modules
-------------

Use the command ``ceph mgr module ls`` to see which modules are available, and
which are currently enabled. Use ``ceph mgr module ls --format=json-pretty`` to
view detailed metadata about disabled modules. Enable or disable modules using
the commands ``ceph mgr module enable <module>`` and ``ceph mgr module disable
<module>`` respectively.

If a module is *enabled* then the active ceph-mgr daemon will load and execute
it.  In the case of modules that provide a service, such as an HTTP server, the
module may publish its address when it is loaded.  To see the addresses of such
modules, use the command ``ceph mgr services``.

Some modules may also implement a special standby mode which runs on standby
ceph-mgr daemons as well as the active daemon.  This enables modules that
provide services to redirect their clients to the active daemon, if the client
tries to connect to a standby.

Consult the documentation pages for individual manager modules for more
information about what functionality each module provides.

Here is an example of enabling the :term:`Dashboard` module:

.. prompt:: bash $

   ceph mgr module ls

.. code-block:: console

	{
		"enabled_modules": [
			"status"
		],
		"disabled_modules": [
			"dashboard"
		]
	}


.. prompt:: bash $

   ceph mgr module enable dashboard
   ceph mgr module ls

.. code-block:: console

	{
		"enabled_modules": [
			"status",
			"dashboard"
		],
		"disabled_modules": [
		]
	}

.. prompt:: bash $

   ceph mgr services

.. code-block:: console

	{
		"dashboard": "http://myserver.com:7789/"
	}


The first time the cluster starts, it uses the :confval:`mgr_initial_modules`
setting to override which modules to enable.  However, this setting is ignored
through the rest of the lifetime of the cluster: only use it for bootstrapping.
For example, before starting your monitor daemons for the first time, you might
add a section like this to your ``ceph.conf``:

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
the module.:

.. prompt:: bash #

   ceph <command | help>

Configuration
-------------

.. confval:: mgr_module_path
.. confval:: mgr_initial_modules
.. confval:: mgr_disabled_modules
.. confval:: mgr_standby_modules
.. confval:: mgr_data
.. confval:: mgr_tick_period
.. confval:: mon_mgr_beacon_grace

.. _Modifying User Capabilities: ../../rados/operations/user-management/#modify-user-capabilities
