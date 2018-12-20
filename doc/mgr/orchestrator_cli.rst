
.. _orchestrator-cli-module:

================
Orchestrator CLI
================

This module provides a command line interface (CLI) to orchestrator
modules (ceph-mgr modules which interface with external orchestation services)

As the orchestrator CLI unifies different external orchestrators, a common nomenclature
for the orchrstrator module is needed.

+--------------------------------------+--------------------------------------+
| host                                 | hostname (not DNS name) on the       |
|                                      | physical host. Not the podname,      |
|                                      | container name, or hostname inside   |
|                                      | the container.                       |
+--------------------------------------+--------------------------------------+
| service                              | A logical service, e.g., “MDS for    |
|                                      | fs1” or “NFS gateway(s)”. Typically  |
|                                      | comprised of multiple service        |
|                                      | instances on multiple hosts for HA   |
+--------------------------------------+--------------------------------------+
| service instance                     | A single instance of a service.      |
|                                      |  Usually a daemon, but maybe not     |
|                                      | (e.g., might be a kernel service     |
|                                      | like LIO or knfsd or whatever)       |
+--------------------------------------+--------------------------------------+
| daemon                               | A running process on a host; use     |
|                                      | “service instance” instead           |
+--------------------------------------+--------------------------------------+

Configuration
=============

You can select the orchestrator module to use with the ``set backend`` command::

    ceph orchestrator set backend <module>

For example, to enable the Rook orchestrator module and use it with the CLI::

    ceph mgr module enable orchestrator_cli
    ceph mgr module enable rook
    ceph orchestrator set backend rook


You can then check backend is properly configured::

    ceph orchestrator status


Usage
=====

.. warning::

    The orchestrator CLI is unfinished and work in progress. Some commands will not
    exist, or return a different result.

.. note::

    Orchestrator modules may only implement a subset of the commands listed below.
    Also, the implementation of the commands are orchestrator module dependent and will
    differ between implementations.

Status
~~~~~~

::

    ceph orchestrator status

Show current orchestrator mode and high-level status (whether the module able
to talk to it)

Also show any in-progress actions.

..
    Host Management
    ~ ~~~~~~~~~~~~~~

    List hosts associated with the cluster: :

        ceph orchestrator host ls

    Add and remove hosts: :

      ceph orchestrator host add <host>
      ceph orchestrator host rm <host>

    . . note: :

    Removing a host only succeeds, if the host is unused.


OSD Management
~~~~~~~~~~~~~~

List Devices
^^^^^^^^^^^^

Print a list of discovered devices, grouped by node and optionally
filtered to a particular node:

::

    ceph orchestrator device ls [--host=...] [--devname=...] [--refresh]

Create OSDs
^^^^^^^^^^^

Create OSDs on a group of devices on a single host::

    ceph orchestrator osd create <host> <ceph-volume-invocation…>

See :ref:`ceph-volume-invocation… <ceph-volume-overview>` for details. E.g.
``ceph orchestrator osd create host1 lvm create …``

The output of ``osd create`` is not specified and may vary between orchestrator backends.

Decommission an OSD
^^^^^^^^^^^^^^^^^^^
::

    ceph orchestrator osd rm <osd-id>

Removes an OSD from the cluster and the host, if the OSD is marked as
``destroyed``.

..
    Blink Device Lights
    ^^^^^^^^^^^^^^^^^^^
    ::

        ceph orchestrator device ident-on <host> <devname>
        ceph orchestrator device ident-off <host> <devname>
        ceph orchestrator device fault-on <host> <devname>
        ceph orchestrator device fault-off <host> <devname>

        ceph orchestrator osd ident-on {primary,journal,db,wal,all} <osd-id>
        ceph orchestrator osd ident-off {primary,journal,db,wal,all} <osd-id>
        ceph orchestrator osd fault-on {primary,journal,db,wal,all} <osd-id>
        ceph orchestrator osd fault-off {primary,journal,db,wal,all} <osd-id>

    Where ``journal`` is the filestore journal, ``wal`` is the write ahead log of
    bluestore and ``all`` stands for all devices associated with the osd


..
    Monitor and manager management
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    ::

        ceph orchestrator mon update <num> [host...]
        ceph orchestrator mgr update <num> [host...]

    Creates or removes MONs or MGRs from the cluster. Orchestrator may return an
    error if it doesn't know how to do this transition.

    .. note::

        The host lists are the new full list of mon/mgr hosts

    .. note::

        specifying hosts is optional for some orchestrator modules
        and mandatory for others (e.g. Ansible).

Service Management
~~~~~~~~~~~~~~~~~~

Service Status
^^^^^^^^^^^^^^

Print a list of services known to the orchestrator. The list can be limited to
services on a particular host with the optional --host parameter and/or
services of a particular type via optional --type parameter
(mon, osd, mgr, mds, rgw):

::

    ceph orchestrator service ls [--host host] [--type type] [--refresh\|--no-cache]

Discover the status of a particular service::

    ceph orchestrator service status <type> <name> [--refresh]


Query the status of a particular service instance (mon, osd, mds, rgw).  For OSDs
the id is the numeric OSD ID, for MDS services it is the filesystem name::

    ceph orchestrator service-instance status <type> <instance-name> [--refresh]



Stateless services (MDS/RGW/NFS/rbd-mirror/iSCSI)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The orchestrator is not responsible for configuring the services. Please look into the corresponding
documentation for details.

The ``name`` parameter is an identifier of the group of instances:
* a CephFS filesystem for a group of MDS daemons,
* a zone name for a group of RGWs

Sizing: the ``size`` parameter gives the number of daemons in the cluster
(e.g. the number of MDS daemons for a particular CephFS filesystem).

Creating/growing/shrinking services::

    ceph orchestrator {mds,rgw} update <name> <size> [host…]
    ceph orchestrator {mds,rgw} add <name>

e.g., ``ceph orchestrator mds update myfs 3 host1 host2 host3``

Start/stop/reload::

    ceph orchestrator service {stop,start,reload} <type> <name>

    ceph orchestrator service-instance {start,stop,reload} <type> <instance-name>


Removing services::

    ceph orchestrator {mds,rgw} rm <name>

