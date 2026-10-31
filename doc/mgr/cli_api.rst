CLI API Commands Module
=======================

The CLI API module exposes most of the ceph-mgr Python API via CLI commands.
This API can be benchmarked.

Enabling
--------

Enable the ``cli api`` module by running the following command:

.. prompt:: bash #

   ceph mgr module enable cli_api

Ensure that the ``cli api`` module is enabled by running the following command:

.. prompt:: bash #

   ceph mgr module ls | grep cli_api

Usage
--------

This the the general form of Manager CLI commands: 

.. prompt:: bash #

   ceph mgr cli <command> <param>

Print the list of servers by running the following command:

.. prompt:: bash #

   ceph mgr cli list_servers

List all available Manager module commands by running the following command:

.. prompt:: bash #

   ceph mgr cli --help

Benchmark a command, by running a command of the following form:

.. prompt:: bash #

   ceph mgr cli_benchmark <number of calls> <number of threads> <command> <param>

For example, run the following command to benchmark the command to get
``osd_map``:

.. prompt:: bash #

   ceph mgr cli_benchmark 100 10 get osd_map


In Umbrella 21 and later releases, you can flush one entry of the Manager's
map cache by running a command of the following form:

.. prompt:: bash #

   ceph mgr cli cache flush <name>

``<name>`` is one of ``osd_map``, ``pg_dump``, ``pg_stats``, ``mon_status``,
``mgr_map``, ``osd_metadata``, ``mds_metadata`` or ``config``. For how the
cache works, see "Performance and Scalability" in
:ref:`mgr-administrator-guide`.
