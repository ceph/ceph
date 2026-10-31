.. _mgr-administrator-guide:

ceph-mgr Administrator's Guide
==============================

Manual Setup
------------

Usually, ``ceph-mgr`` daemons are deployed with :ref:`cephadm`. These
instructions describe how to set up a ``ceph-mgr`` daemon manually.

First, create an authentication key for your daemon:

.. prompt:: bash #

   ceph auth get-or-create mgr.$name mon 'allow profile mgr' osd 'allow *' mds 'allow *'

Place that key as file named ``keyring`` into :confval:`mgr_data` path. Path for a
``ceph-mgr`` named "foo" (``$name`` placeholder replaced by ``foo``) would be ``/var/lib/ceph/mgr/ceph-foo``
and ``keyring`` would then be located in ``/var/lib/ceph/mgr/ceph-foo/keyring``.

Start the ``ceph-mgr`` daemon:

.. prompt:: bash #

   ceph-mgr -i $name

Check that the Manager has come up by looking at the output of ``ceph status``.
The ``services`` section should now include a ``mgr`` line such as::

    mgr: $name(active, since 10s)


Interpreting Manager Daemon Status
----------------------------------

The ``services`` section of the ``ceph status`` output shows each
``ceph-mgr`` daemon in one of three states:

1. ``active``
     This Manager daemon has been fully initialized, which means it is ready to receive
     and execute commands. Only one Manager will be in this state at a time.

2. ``active (starting)``
     This Manager daemon has been chosen to be ``active``, but it is not done initializing.
     Although it is not yet ready to execute commands, an operator may still issue commands,
     which will be held and executed once the Manager becomes ``active``. Only one Manager
     will be in this state at a time.

3. ``standby``
     This Manager daemon is not currently receiving or executing commands, but it is ready to
     take over if the current active Manager becomes unavailable. To make a standby take
     over, fail the active Manager with ``ceph mgr fail``. The Monitors then promote one of
     the standbys; you cannot choose which one. Running ``ceph mgr fail`` with the name of a
     standby fails that standby, not the active Manager. All other
     Manager daemons which are not ``active`` or ``active (starting)`` will be in this state.

Each of these states are visible in the output of the ``ceph status`` command. For example:

.. prompt:: bash #

   ceph status

.. code-block:: console

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


Client Authentication
---------------------

Commands that the Manager handles need Manager capabilities (``mgr`` caps) in
the client's key. If the key's ``mgr`` caps do not allow a command, the command
fails with ``EACCES`` and this message:
``access denied: does your client key have mgr caps?``. To fix this, add a
``mgr`` capability to the key, for example ``mgr 'allow *'`` for an
administrative client, as described in :ref:`modify-user-capabilities`.


High Availability
-----------------

In general, you should set up a ``ceph-mgr`` daemon on each of the hosts
running a ``ceph-mon`` daemon to achieve the same level of availability.

By default, whichever ``ceph-mgr`` instance comes up first will be made
active by the Monitors, and the others will be standbys.  There is
no requirement for quorum among the ``ceph-mgr`` daemons.

If the active daemon fails to send a beacon to the Monitors for
more than :confval:`mon_mgr_beacon_grace`, then it will be replaced
by a standby.

If you want to preempt failover, you can explicitly mark a ``ceph-mgr``
daemon as failed using ``ceph mgr fail <mgr name>``.


Performance and Scalability
---------------------------

In Umbrella 21 and later releases, Manager modules share a cache of cluster
maps, and the cache is enabled by default.

The cache helps most on large clusters, where maps such as the OSDMap are large
and costly to serialize for each request.

A cached map is dropped when the Manager receives a new version of it, for
example a new OSDMap. The cached PG statistics (``pg_dump`` and ``pg_stats``)
and ``mon_status`` are dropped each time the Monitors send the Manager their
status digest, every 5 seconds by default. If needed, you can flush one entry
with ``ceph mgr cli cache flush <name>``, where ``<name>`` is one of
``osd_map``, ``pg_dump``, ``pg_stats``, ``mon_status``, ``mgr_map``,
``osd_metadata``, ``mds_metadata`` or ``config``. If that entry is not cached,
the command reports ``no cached map named <name>``. The command is provided by
the ``cli_api`` module, which is not enabled by default. Enable it first:

.. prompt:: bash #

   ceph mgr module enable cli_api

To disable the cache (not recommended for large clusters), run:
``ceph config set mgr mgr_map_cache_enabled false``

You can run ``ceph daemon mgr.${MGRNAME} perf dump`` to retrieve
perf counters of a Manager module. In ``mgr.cache_hit`` and ``mgr.cache_miss``
you'll find the hit/miss ratio of the Manager cache, which can help verify the
cache is operating effectively.

In Umbrella 21 and later releases, the Manager includes a ThreadMonitor that
measures, for each enabled module, the CPU usage of two threads (the thread
that runs the module's notify and command handlers, and its ``serve()``
thread) and the memory usage (RSS) of the whole ``ceph-mgr`` process. Threads
that a module starts itself, such as web server worker threads, are not
measured. This monitoring can be configured with
``ceph config set mgr mgr_module_monitor_interval <seconds>``, where ``seconds``
is the monitoring interval. Setting this to 0 disables module monitoring.

The ThreadMonitor provides per-module performance counters accessible via
``ceph daemon mgr.${MGRNAME} perf dump``, including:

- ``notify_avg_usec``: Average time spent in notify calls (microseconds)
- ``cmd_avg_usec``: Average time spent in command calls (microseconds)  
- ``alive``: 1 while the module's ``serve()`` method runs, and 0 after it
  returns or the module shuts down. Modules that do not implement
  ``serve()`` report 0, and a module whose ``serve()`` failed with an
  exception keeps reporting 1, so this counter does not show whether a
  module works. A failed module raises the ``MGR_MODULE_ERROR`` health check.
- ``cpu_usage``: CPU percentage for the main module thread
- ``serve_cpu_usage``: CPU percentage for the module's serve thread (if present)
- ``mem_rss_current``: Current process memory usage (RSS) in bytes
- ``mem_rss_change``: Memory usage change since last measurement in bytes

The CPU counters help identify modules that use a lot of CPU in those threads.
The memory counters describe the whole ``ceph-mgr`` process and show the same
values in every module's section, so use them to watch the daemon for memory
growth, not to find which module is responsible. The ``notify_avg_usec`` and
``cmd_avg_usec`` counters track the performance of module operations, while
the CPU and memory counters monitor resource consumption. The default monitoring
interval is 5 seconds.


Automatic Stats Period Tuning
------------------------------

Beginning with Squid 19.2.7 and Tentacle 20.2.3, the Manager automatically
adjusts :confval:`mgr_stats_period` based on message queue depth to prevent
overload during high cluster activity. This feature is enabled by default and
can be controlled with the following settings:

- :confval:`mgr_stats_period_autotune` : Enable or disable
  automatic tuning of the stats period.
- :confval:`mgr_stats_period_autotune_queue_threshold` :
  The message queue depth threshold that triggers an increase in the stats period.

When the queue depth exceeds this threshold, the stats period is increased to
reduce load. Conversely, if the queue depth remains low and the stats period is
above the baseline, the period is decreased to improve responsiveness.
Automatic tuning never raises the stats period above 60 seconds. If you set
:confval:`mgr_stats_period` yourself, that value becomes the new baseline
and is not capped.


Using Modules
-------------

Use the command ``ceph mgr module ls`` to see which modules are available, and
which are currently enabled. Use ``ceph mgr module ls --format=json-pretty`` to
view detailed metadata about disabled modules. Enable or disable modules using
the commands ``ceph mgr module enable <module>`` and ``ceph mgr module disable
<module>`` respectively.

If a module is *enabled* then the active ``ceph-mgr`` daemon will load and execute
it.  In the case of modules that provide a service, such as an HTTP server, the
module may publish its address when it is loaded.  To see the addresses of such
modules, use the command ``ceph mgr services``.

Some modules may also implement a special standby mode which runs on standby
``ceph-mgr`` daemons as well as the active daemon.  This enables modules that
provide services to redirect their clients to the active daemon, if the client
tries to connect to a standby.

Consult the documentation pages for individual Manager modules for more
information about what functionality each module provides.

Here is an example of enabling the :term:`Dashboard` module. ``ceph mgr module ls``
prints a table: modules that are always on are marked ``on (always on)``,
other enabled modules ``on``, and disabled modules ``-`` (output shortened):

.. prompt:: bash #

   ceph mgr module ls

.. code-block:: console

   MODULE
   balancer              on (always on)
   crash                 on (always on)
   ...
   iostat                on
   nfs                   on
   ...
   dashboard             -
   ...


.. prompt:: bash #

   ceph mgr module enable dashboard
   ceph mgr module ls

.. code-block:: console

   MODULE
   balancer              on (always on)
   ...
   dashboard             on
   iostat                on
   ...

.. prompt:: bash #

   ceph mgr services

.. code-block:: console

   {
       "dashboard": "https://192.0.2.10:8443/"
   }

With its default settings, the Dashboard serves HTTPS on port 8443 and
publishes the IP address of the active Manager. It publishes its address only
after a TLS certificate is configured (see :ref:`dashboard-ssl-tls-support`);
until then, ``ceph mgr services`` does not list it.


The first time the cluster starts, it uses the :confval:`mgr_initial_modules`
setting to override which modules to enable.  However, this setting is ignored
through the rest of the lifetime of the cluster: only use it for bootstrapping.
For example, before starting your Monitor daemons for the first time, you might
add a section like this to your ``ceph.conf``:

.. code-block:: ini

    [mon]
        mgr_initial_modules = iostat nfs dashboard

The value replaces the default list, so include any default module that you
want to keep. The default list depends on the release: for example, Squid 19.2
also enables ``restful``, and Tentacle 20.2.3 and later releases also enable
``nvmeof``. This example keeps ``iostat`` and ``nfs``, which every supported
release enables by default, and adds the Dashboard. Always-on modules such as
``balancer`` are enabled whatever the list contains and do not need to be
listed.


Module Pool
-----------

The Manager creates a pool for use by its modules to store state. The name of
this pool is ``.mgr`` (with the leading ``.`` indicating a reserved pool
name).


Calling Module Commands
-----------------------

Where a module implements command line hooks, the commands will
be accessible as ordinary Ceph commands.  Ceph will automatically incorporate
module commands into the standard CLI interface and route them appropriately to
the module. To list all commands, including module commands, run:

.. prompt:: bash #

   ceph -h

To list only the commands that start with a given prefix, put the prefix
before ``-h``, for example ``ceph orch -h``.


Configuration
-------------

.. confval:: mgr_module_path
.. confval:: mgr_initial_modules
.. confval:: mgr_disabled_modules
.. confval:: mgr_standby_modules
.. confval:: mgr_data
.. confval:: mgr_tick_period
.. confval:: mon_mgr_beacon_grace
.. confval:: mgr_stats_period
.. confval:: mgr_stats_period_autotune
.. confval:: mgr_stats_period_autotune_queue_threshold
