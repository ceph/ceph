
.. _orchestrator-cli-module:

================
Orchestrator CLI
================

This module provides a command line interface (CLI) to orchestrator
modules (ceph-mgr modules which interface with external orchestation services)

Configuration
=============

You can select the orchestrator module to use with the ``set backend`` command:

::

    ceph orchestrator set backend <module>

For example, to enable the Rook orchestrator module and use it with the CLI:

::

    ceph mgr module enable orchestrator_cli
    ceph mgr module enable rook
    ceph orchestrator set backend rook


Usage
=====

Print a list of discovered devices, grouped by node and optionally
filtered to a particular node:

::

    orchestrator device ls [node]

Query the status of a particular service (mon, osd, mds, rgw).  For OSDs
the id is the numeric OSD ID, for MDS services it is the filesystem name:

::

    orchestrator service status <type> <id>

Create a service.  For an OSD, the "what" is <node>:<device>, where the
device naming should match what was reported in ``device ls``.  For an MDS
service, the "what" is the filesystem name:

::

    orchestrator service add <type> <what>


