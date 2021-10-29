CLI API Commands Module
=======================

The CLI API module exposes most ceph-mgr python API via CLI. Furthermore, this API can be
benchmarked for further testing.

Enabling
--------

The *cli api commands* module is enabled with::

  ceph mgr module enable cli_api_commands

To check that it is enabled, run::

  ceph mgr module ls

Usage
--------

To run a mgr module command, run::

  ceph mgr cli <command> <param>

For example, use the following command to print the list of servers::

  ceph mgr cli list_servers

List all available mgr module commands with , run::

  ceph mgr cli --help

To benchmark a command, run::

  ceph mgr cli benchmark <command> <param> <number of calls> <number of threads>

For example, use the following command to benchmark the call of osd_maps::

  ceph mgr cli benchmark get osd_map 100 10
