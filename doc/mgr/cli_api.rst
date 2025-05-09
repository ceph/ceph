CLI API Commands Module
=======================

The CLI API module exposes most of the ceph-mgr Python API via CLI.  This API
can be benchmarked for further testing.

Enabling
--------

Enable the ``cli api commands`` module by running the following command:

.. prompt:: bash #

   ceph mgr module enable cli_api

Ensure that the ``cli api commands`` module is enabled by running the following
command:

.. prompt:: bash #

   ceph mgr module ls | grep cli_api

Usage
--------

This the the general form of Manager module commands: 

.. prompt:: bash #

   ceph mgr cli <command> <param>

Print the list of servers by running the following command:

.. prompt:: bash #

   ceph mgr cli list_servers

List all available mgr module commands by running the following command:

.. prompt:: bash #

   ceph mgr cli --help

Benchmark a command, by running a command of the following form:

.. prompt:: bash #

   ceph mgr cli_benchmark <number of calls> <number of threads> <command> <param>

For example, run the following command to benchmark the command to get
``osd_map``:

.. prompt:: bash #

   ceph mgr cli_benchmark 100 10 get osd_map
