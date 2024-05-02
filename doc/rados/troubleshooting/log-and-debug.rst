=======================
 Logging and Debugging
=======================

Ceph component debug log levels can be adjusted at runtime, while services are
running. In some circumstances you might want to adjust debug log levels in
``ceph.conf`` or in the central config store. Increased debug logging can be
useful if you are encountering issues when operating your cluster.  By default,
Ceph log files are in ``/var/log/ceph``.

.. tip:: Remember that debug output can slow down your system, and that this
   latency sometimes hides race conditions.

Debug logging is resource intensive. If you encounter a problem in a specific
component of your cluster, begin troubleshooting by enabling logging for only
that component of the cluster. For example, if your OSDs are running without
errors, but your metadata servers are not, enable logging for any specific
metadata server instances that are having problems. Continue by enabling
logging for each subsystem only as needed.

.. important:: Verbose logging sometimes generates over 1 GB of data per hour.
   If the disk that your operating system runs on (your "OS disk") reaches its
   capacity, the node associated with that disk will stop working.

Whenever you enable or increase the rate of debug logging, make sure that you
have ample capacity for log files, as this may dramatically increase their
size.  For details on rotating log files, see `Accelerating Log Rotation`_.
When your system is running well again, remove unnecessary debugging settings
in order to ensure that your cluster runs optimally. Logging debug-output
messages is a slow process and a potential waste of your cluster's resources.

For details on available settings, see `Subsystem, Log and Debug Settings`_.

Runtime
=======

To see the configuration settings at runtime, log in to a host that has a
running daemon and run a command of the following form:

.. prompt:: bash $

   ceph daemon {daemon-name} config show | less

For example:

.. prompt:: bash $

   ceph daemon osd.0 config show | less

To activate Ceph's debugging output (that is, the ``dout()`` logging function)
at runtime, inject arguments into the runtime configuration by running a ``ceph
tell`` command of the following form:

..  prompt:: bash $

    ceph tell {daemon-type}.{daemon id or *} config set {name} {value}

Here ``{daemon-type}`` is ``osd``, ``mon``, or ``mds``. Apply the runtime
setting either to a specific daemon (by specifying its ID) or to all daemons of
a particular type (by using the ``*`` operator).  For example, to increase
debug logging for a specific ``ceph-osd`` daemon named ``osd.0``, run the
following command:

..  prompt:: bash $

    ceph tell osd.0 config set debug_osd 0/5

The ``ceph tell`` command goes through the monitors. However, if you are unable
to bind to the monitor, there is another method that can be used to activate
Ceph's debugging output: use the ``ceph daemon`` command to log in to the host
of a specific daemon and change the daemon's configuration. For example:

.. prompt:: bash $

   sudo ceph daemon osd.0 config set debug_osd 0/5

For details on available settings, see `Subsystem, Log and Debug Settings`_.


Boot Time
=========

To activate Ceph's debugging output (that is, the ``dout()`` logging function)
at boot time, you must add settings to your Ceph configuration file.
Subsystems that are common to all daemons are set under ``[global]`` in the
configuration file. Subsystems for a specific daemon are set under the relevant
daemon section in the configuration file (for example, ``[mon]``, ``[osd]``,
``[mds]``). Here is an example that shows possible debugging settings in a Ceph
configuration file:

.. code-block:: ini

    [global]
        debug_ms = 1/5
        
    [mon]
        debug_mon = 20
        debug_paxos = 1/5
        debug_auth = 2
         
     [osd]
         debug_osd = 1/5
         debug_filestore = 1/5
         debug_journal = 1
         debug_monc = 5/20
         
    [mds]
        debug_mds = 1
        debug_mds_balancer = 1


For details, see `Subsystem, Log and Debug Settings`_.


Accelerating Log Rotation
=========================

If your log filesystem is nearly full, you can accelerate log rotation by
modifying the Ceph log rotation file at ``/etc/logrotate.d/ceph``. To increase
the frequency of log rotation (which will guard against a filesystem reaching
capacity), add a ``size`` directive after the ``weekly`` frequency directive.
To smooth out volume spikes, consider changing ``weekly`` to ``daily`` and
consider changing ``rotate`` to ``30``. The procedure for adding the size
setting is shown immediately below. 

#. Note the default settings of the ``/etc/logrotate.d/ceph`` file::

      rotate 7
      weekly
      compress
      sharedscripts

#. Modify them by adding a ``size`` setting::

      rotate 7
      weekly
      size 500M
      compress
      sharedscripts

#. Start the crontab editor for your user space:

   .. prompt:: bash $

      crontab -e

#. Add an entry to crontab that instructs cron to check the
   ``etc/logrotate.d/ceph`` file::

      30 * * * * /usr/sbin/logrotate /etc/logrotate.d/ceph >/dev/null 2>&1

In this example, the ``etc/logrotate.d/ceph`` file will be checked every 30
minutes.

Valgrind
========

When you are debugging your cluster's performance, you might find it necessary
to track down memory and threading issues. The Valgrind tool suite can be used
to detect problems in a specific daemon, in a particular type of daemon, or in
the entire cluster. Because Valgrind is computationally expensive, it should be
used only when developing or debugging Ceph, and it will slow down your system
if used at other times. Valgrind messages are logged to ``stderr``. 


Subsystem, Log and Debug Settings
=================================

Debug logging output is typically enabled via subsystems. 

Ceph Subsystems
---------------

For each subsystem, there is a logging level for its output logs (a so-called
"log level") and a logging level for its in-memory logs (a so-called "memory
level"). Different values may be set for these two logging levels in each
subsystem. Ceph's logging levels operate on a scale of ``1`` to ``20``, where
``1`` is terse and ``20`` is verbose.  In certain rare cases, there are logging
levels that can take a value greater than 20. The resulting logs are extremely
verbose.

The in-memory logs are not sent to the output log unless one or more of the
following conditions are true:

- a fatal signal has been raised or
- an assertion within Ceph code has been triggered or
- the sending of in-memory logs to the output log has been manually triggered.
  Consult `the portion of the "Ceph Administration Tool documentation
  that provides an example of how to submit admin socket commands
  <http://docs.ceph.com/en/latest/man/8/ceph/#daemon>`_ for more detail.

Log levels and memory levels can be set either together or separately. If a
subsystem is assigned a single value, then that value determines both the log
level and the memory level. For example, ``debug ms = 5`` will give the ``ms``
subsystem a log level of ``5`` and a memory level of ``5``.  On the other hand,
if a subsystem is assigned two values that are separated by a forward slash
(/), then the first value determines the log level and the second value
determines the memory level. For example, ``debug ms = 1/5`` will give the
``ms`` subsystem a log level of ``1`` and a memory level of ``5``. See the
following:

.. code-block:: ini 

    debug {subsystem} = {log-level}/{memory-level}
    #for example
    debug mds balancer = 1/20

The following table provides a list of Ceph subsystems and their default log and
memory levels. Once you complete your logging efforts, restore the subsystems
to their default level or to a level suitable for normal operations.

+--------------------------+-----------+--------------+
| Subsystem                | Log Level | Memory Level |
+==========================+===========+==============+
| ``default``              |     0     |      5       |
+--------------------------+-----------+--------------+
| ``lockdep``              |     0     |      1       |
+--------------------------+-----------+--------------+
| ``context``              |     0     |      1       |
+--------------------------+-----------+--------------+
| ``crush``                |     1     |      1       |
+--------------------------+-----------+--------------+
| ``mds``                  |     1     |      5       |
+--------------------------+-----------+--------------+
| ``mds balancer``         |     1     |      5       |
+--------------------------+-----------+--------------+
| ``mds log``              |     1     |      5       |
+--------------------------+-----------+--------------+
| ``mds log expire``       |     1     |      5       |
+--------------------------+-----------+--------------+
| ``mds migrator``         |     1     |      5       |
+--------------------------+-----------+--------------+
| ``buffer``               |     0     |      1       |
+--------------------------+-----------+--------------+
| ``timer``                |     0     |      1       |
+--------------------------+-----------+--------------+
| ``filer``                |     0     |      1       |
+--------------------------+-----------+--------------+
| ``striper``              |     0     |      1       |
+--------------------------+-----------+--------------+
| ``objecter``             |     0     |      1       |
+--------------------------+-----------+--------------+
| ``rados``                |     0     |      5       |
+--------------------------+-----------+--------------+
| ``rbd``                  |     0     |      5       |
+--------------------------+-----------+--------------+
| ``rbd mirror``           |     0     |      5       |
+--------------------------+-----------+--------------+
| ``rbd replay``           |     0     |      5       |
+--------------------------+-----------+--------------+
| ``rbd pwl``              |     0     |      5       |
+--------------------------+-----------+--------------+
| ``journaler``            |     0     |      5       |
+--------------------------+-----------+--------------+
| ``objectcacher``         |     0     |      5       |
+--------------------------+-----------+--------------+
| ``immutable obj cache``  |     0     |      5       |
+--------------------------+-----------+--------------+
| ``client``               |     0     |      5       |
+--------------------------+-----------+--------------+
| ``osd``                  |     1     |      5       |
+--------------------------+-----------+--------------+
| ``optracker``            |     0     |      5       |
+--------------------------+-----------+--------------+
| ``objclass``             |     0     |      5       |
+--------------------------+-----------+--------------+
| ``filestore``            |     1     |      3       |
+--------------------------+-----------+--------------+
| ``journal``              |     1     |      3       |
+--------------------------+-----------+--------------+
| ``ms``                   |     0     |      5       |
+--------------------------+-----------+--------------+
| ``mon``                  |     1     |      5       |
+--------------------------+-----------+--------------+
| ``monc``                 |     0     |      10      |
+--------------------------+-----------+--------------+
| ``paxos``                |     1     |      5       |
+--------------------------+-----------+--------------+
| ``tp``                   |     0     |      5       |
+--------------------------+-----------+--------------+
| ``auth``                 |     1     |      5       |
+--------------------------+-----------+--------------+
| ``crypto``               |     1     |      5       |
+--------------------------+-----------+--------------+
| ``finisher``             |     1     |      1       |
+--------------------------+-----------+--------------+
| ``reserver``             |     1     |      1       |
+--------------------------+-----------+--------------+
| ``heartbeatmap``         |     1     |      5       |
+--------------------------+-----------+--------------+
| ``perfcounter``          |     1     |      5       |
+--------------------------+-----------+--------------+
| ``rgw``                  |     1     |      5       |
+--------------------------+-----------+--------------+
| ``rgw sync``             |     1     |      5       |
+--------------------------+-----------+--------------+
| ``rgw datacache``        |     1     |      5       |
+--------------------------+-----------+--------------+
| ``rgw access``           |     1     |      5       |
+--------------------------+-----------+--------------+
| ``rgw dbstore``          |     1     |      5       |
+--------------------------+-----------+--------------+
| ``rgw lifecycle``        |     1     |      5       |
+--------------------------+-----------+--------------+
| ``rgw notification``     |     1     |      5       |
+--------------------------+-----------+--------------+
| ``javaclient``           |     1     |      5       |
+--------------------------+-----------+--------------+
| ``asok``                 |     1     |      5       |
+--------------------------+-----------+--------------+
| ``throttle``             |     1     |      1       |
+--------------------------+-----------+--------------+
| ``refs``                 |     0     |      0       |
+--------------------------+-----------+--------------+
| ``compressor``           |     1     |      5       |
+--------------------------+-----------+--------------+
| ``bluestore``            |     1     |      5       |
+--------------------------+-----------+--------------+
| ``bluefs``               |     1     |      5       |
+--------------------------+-----------+--------------+
| ``bdev``                 |     1     |      3       |
+--------------------------+-----------+--------------+
| ``kstore``               |     1     |      5       |
+--------------------------+-----------+--------------+
| ``rocksdb``              |     4     |      5       |
+--------------------------+-----------+--------------+
| ``fuse``                 |     1     |      5       |
+--------------------------+-----------+--------------+
| ``mgr``                  |     2     |      5       |
+--------------------------+-----------+--------------+
| ``mgrc``                 |     1     |      5       |
+--------------------------+-----------+--------------+
| ``dpdk``                 |     1     |      5       |
+--------------------------+-----------+--------------+
| ``eventtrace``           |     1     |      5       |
+--------------------------+-----------+--------------+
| ``prioritycache``        |     1     |      5       |
+--------------------------+-----------+--------------+
| ``test``                 |     0     |      5       |
+--------------------------+-----------+--------------+
| ``cephfs mirror``        |     0     |      5       |
+--------------------------+-----------+--------------+
| ``cephsqlite``           |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore``             |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore onode``       |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore odata``       |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore ompap``       |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore tm``          |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore t``           |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore cleaner``     |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore epm``         |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore lba``         |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore fixedkv tree``|     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore cache``       |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore journal``     |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore device``      |     0     |      5       |
+--------------------------+-----------+--------------+
| ``seastore backref``     |     0     |      5       |
+--------------------------+-----------+--------------+
| ``alienstore``           |     0     |      5       |
+--------------------------+-----------+--------------+
| ``mclock``               |     1     |      5       |
+--------------------------+-----------+--------------+
| ``cyanstore``            |     0     |      5       |
+--------------------------+-----------+--------------+
| ``ceph exporter``        |     1     |      5       |
+--------------------------+-----------+--------------+
| ``memstore``             |     1     |      5       |
+--------------------------+-----------+--------------+
| ``trace``                |     1     |      5       |
+--------------------------+-----------+--------------+


Logging and Debugging Settings
------------------------------

It is not necessary to specify logging and debugging settings in the Ceph
configuration file, but you may override default settings when needed. Ceph
supports the following settings:

.. confval:: log_file
.. confval:: log_max_new
.. confval:: log_max_recent
.. confval:: log_to_file
.. confval:: log_to_stderr
.. confval:: err_to_stderr
.. confval:: log_to_syslog
.. confval:: err_to_syslog
.. confval:: log_flush_on_exit
.. confval:: clog_to_monitors
.. confval:: clog_to_syslog
.. confval:: mon_cluster_log_to_syslog
.. confval:: mon_cluster_log_file

OSD
---

.. confval:: osd_debug_drop_ping_probability
.. confval:: osd_debug_drop_ping_duration

Filestore
---------

.. confval:: filestore_debug_omap_check

MDS
---

- :confval:`mds_debug_scatterstat`
- :confval:`mds_debug_frag`
- :confval:`mds_debug_auth_pins`
- :confval:`mds_debug_subtrees`

RADOS Gateway
-------------

- :confval:`rgw_log_nonexistent_bucket`
- :confval:`rgw_log_object_name`
- :confval:`rgw_log_object_name_utc`
- :confval:`rgw_enable_ops_log`
- :confval:`rgw_enable_usage_log`
- :confval:`rgw_usage_log_flush_threshold`
- :confval:`rgw_usage_log_tick_interval`
