
.. _orchestrator-cli-module:

================
Orchestrator CLI
================

This module provides a command line interface (CLI) to orchestrator
modules (ceph-mgr modules which interface with external orchestration services).

As the orchestrator CLI unifies different external orchestrators, a common nomenclature
for the orchestrator module is needed.

+--------------------------------------+---------------------------------------+
| *host*                               | hostname (not DNS name) of the        |
|                                      | physical host. Not the podname,       |
|                                      | container name, or hostname inside    |
|                                      | the container.                        |
+--------------------------------------+---------------------------------------+
| *service type*                       | The type of the service. e.g., nfs,   |
|                                      | mds, osd, mon, rgw, mgr, iscsi        |
+--------------------------------------+---------------------------------------+
| *service*                            | A logical service, Typically          |
|                                      | comprised of multiple service         |
|                                      | instances on multiple hosts for HA    |
|                                      |                                       |
|                                      | * ``fs_name`` for mds type            |
|                                      | * ``rgw_zone`` for rgw type           |
|                                      | * ``ganesha_cluster_id`` for nfs type |
+--------------------------------------+---------------------------------------+
| *daemon*                             | A single instance of a service.       |
|                                      | Usually a daemon, but maybe not       |
|                                      | (e.g., might be a kernel service      |
|                                      | like LIO or knfsd or whatever)        |
|                                      |                                       |
|                                      | This identifier should                |
|                                      | uniquely identify the instance        |
+--------------------------------------+---------------------------------------+

The relation between the names is the following:

* A *service* has a specfic *service type*
* A *daemon* is a physical instance of a *service type*


.. note::

    Orchestrator modules may only implement a subset of the commands listed below.
    Also, the implementation of the commands are orchestrator module dependent and will
    differ between implementations.

Status
======

::

    ceph orch status

Show current orchestrator mode and high-level status (whether the module able
to talk to it)

Also show any in-progress actions.

Host Management
===============

List hosts associated with the cluster::

    ceph orch host ls

Add and remove hosts::

    ceph orch host add <host>
    ceph orch host rm <host>

OSD Management
==============

List Devices
------------

Print a list of discovered devices, grouped by host and optionally
filtered to a particular host:

::

    ceph orch device ls [--host=...] [--refresh]

Example::

    # ceph orch device ls
    Host 192.168.121.206:
    Device Path           Type       Size    Rotates  Available Model
    /dev/sdb               hdd      50.0G       True       True ATA/QEMU HARDDISK
    /dev/sda               hdd      50.0G       True      False ATA/QEMU HARDDISK

    Host 192.168.121.181:
    Device Path           Type       Size    Rotates  Available Model
    /dev/sdb               hdd      50.0G       True       True ATA/QEMU HARDDISK
    /dev/sda               hdd      50.0G       True      False ATA/QEMU HARDDISK

.. note::
    Output form Ansible orchestrator

Create OSDs
-----------

Create OSDs on a group of devices on a single host::

    ceph orch osd create <host>:<drive>
    ceph orch osd create -i <path-to-drive-group.json>


The output of ``osd create`` is not specified and may vary between orchestrator backends.

Where ``drive.group.json`` is a JSON file containing the fields defined in
:class:`ceph.deployment_utils.drive_group.DriveGroupSpec`

Example::

    # ceph orch osd create 192.168.121.206:/dev/sdc
    {"status": "OK", "msg": "", "data": {"event": "playbook_on_stats", "uuid": "7082f3ba-f5b7-4b7c-9477-e74ca918afcb", "stdout": "\r\nPLAY RECAP *********************************************************************\r\n192.168.121.206            : ok=96   changed=3    unreachable=0    failed=0   \r\n", "counter": 932, "pid": 10294, "created": "2019-05-28T22:22:58.527821", "end_line": 1170, "runner_ident": "083cad3c-8197-11e9-b07a-2016b900e38f", "start_line": 1166, "event_data": {"ignored": 0, "skipped": {"192.168.121.206": 186}, "ok": {"192.168.121.206": 96}, "artifact_data": {}, "rescued": 0, "changed": {"192.168.121.206": 3}, "pid": 10294, "dark": {}, "playbook_uuid": "409364a6-9d49-4e44-8b7b-c28e5b3adf89", "playbook": "add-osd.yml", "failures": {}, "processed": {"192.168.121.206": 1}}, "parent_uuid": "409364a6-9d49-4e44-8b7b-c28e5b3adf89"}}

.. note::
    Output form Ansible orchestrator

Decommission an OSD
-------------------
::

    ceph orch osd rm <osd-id> [osd-id...]

Removes one or more OSDs from the cluster and the host, if the OSDs are marked as
``destroyed``.

Example::

    # ceph orch osd rm 4
    {"status": "OK", "msg": "", "data": {"event": "playbook_on_stats", "uuid": "1a16e631-906d-48e0-9e24-fa7eb593cc0a", "stdout": "\r\nPLAY RECAP *********************************************************************\r\n192.168.121.158            : ok=2    changed=0    unreachable=0    failed=0   \r\n192.168.121.181            : ok=2    changed=0    unreachable=0    failed=0   \r\n192.168.121.206            : ok=2    changed=0    unreachable=0    failed=0   \r\nlocalhost                  : ok=31   changed=8    unreachable=0    failed=0   \r\n", "counter": 240, "pid": 10948, "created": "2019-05-28T22:26:09.264012", "end_line": 308, "runner_ident": "8c093db0-8197-11e9-b07a-2016b900e38f", "start_line": 301, "event_data": {"ignored": 0, "skipped": {"localhost": 37}, "ok": {"192.168.121.181": 2, "192.168.121.158": 2, "192.168.121.206": 2, "localhost": 31}, "artifact_data": {}, "rescued": 0, "changed": {"localhost": 8}, "pid": 10948, "dark": {}, "playbook_uuid": "a12ec40e-bce9-4bc9-b09e-2d8f76a5be02", "playbook": "shrink-osd.yml", "failures": {}, "processed": {"192.168.121.181": 1, "192.168.121.158": 1, "192.168.121.206": 1, "localhost": 1}}, "parent_uuid": "a12ec40e-bce9-4bc9-b09e-2d8f76a5be02"}}

.. note::
    Output form Ansible orchestrator

..
    Blink Device Lights
    ^^^^^^^^^^^^^^^^^^^
    ::

        ceph orch device ident-on <dev_id>
        ceph orch device ident-on <dev_name> <host>
        ceph orch device fault-on <dev_id>
        ceph orch device fault-on <dev_name> <host>

        ceph orch device ident-off <dev_id> [--force=true]
        ceph orch device ident-off <dev_id> <host> [--force=true]
        ceph orch device fault-off <dev_id> [--force=true]
        ceph orch device fault-off <dev_id> <host> [--force=true]

    where ``dev_id`` is the device id as listed in ``osd metadata``,
    ``dev_name`` is the name of the device on the system and ``host`` is the host as
    returned by ``orchestrator host ls``

        ceph orch osd ident-on {primary,journal,db,wal,all} <osd-id>
        ceph orch osd ident-off {primary,journal,db,wal,all} <osd-id>
        ceph orch osd fault-on {primary,journal,db,wal,all} <osd-id>
        ceph orch osd fault-off {primary,journal,db,wal,all} <osd-id>

    Where ``journal`` is the filestore journal, ``wal`` is the write ahead log of
    bluestore and ``all`` stands for all devices associated with the osd


Monitor and manager management
==============================

Creates or removes MONs or MGRs from the cluster. Orchestrator may return an
error if it doesn't know how to do this transition.

Update the number of monitor hosts::

    ceph orch apply mon <num> [host, host:network...]

Each host can optionally specify a network for the monitor to listen on.

Update the number of manager hosts::

    ceph orch apply mgr <num> [host...]

..
    .. note::

        The host lists are the new full list of mon/mgr hosts

    .. note::

        specifying hosts is optional for some orchestrator modules
        and mandatory for others (e.g. Ansible).


Service Status
==============

Print a list of services known to the orchestrator. The list can be limited to
services on a particular host with the optional --host parameter and/or
services of a particular type via optional --type parameter
(mon, osd, mgr, mds, rgw):

::

    ceph orch ps
    ceph orch service ls [--host host] [--svc_type type] [--refresh]

Discover the status of a particular service or daemons::

    ceph orch service ls --svc_type type --svc_id <name> [--refresh]


Query the status of a particular service instance (mon, osd, mds, rgw).  For OSDs
the id is the numeric OSD ID, for MDS services it is the file system name::

    ceph orch daemon status <type> <instance-name> [--refresh]



Stateless services (MDS/RGW/NFS/rbd-mirror/iSCSI)
=================================================

The orchestrator is not responsible for configuring the services. Please look into the corresponding
documentation for details.

The ``name`` parameter is an identifier of the group of instances:

* a CephFS file system for a group of MDS daemons,
* a zone name for a group of RGWs

Sizing: the ``size`` parameter gives the number of daemons in the cluster
(e.g. the number of MDS daemons for a particular CephFS file system).

Creating/growing/shrinking/removing services::

    ceph orch {mds,rgw} update <name> <size> [host…]
    ceph orch {mds,rgw} add <name>
    ceph orch nfs update <name> <size> [host…]
    ceph orch nfs add <name> <pool> [--namespace=<namespace>]
    ceph orch {mds,rgw,nfs} rm <name>

e.g., ``ceph orch mds update myfs 3 host1 host2 host3``

Start/stop/reload::

    ceph orch service {stop,start,reload} <type> <name>

    ceph orch daemon {start,stop,reload} <type> <daemon-id>
    

.. _orchestrator-cli-placement-spec:
    
Placement Specification
=======================

In order to allow the orchestrator to deploy a *service*, it needs to
know how many and where it should deploy *daemons*. The orchestrator 
defines a placement specification:

* To deploy three *daemons*, simply specify the count: ``3``
* To deploy *daemons* on specific hosts, specify the host names: ``host1 host2 host3``
* To deploy *daemons* on a subset of hosts, also specify the count: ``2 host1 host2 host3``

    
Configuring the Orchestrator CLI
================================

To enable the orchestrator, select the orchestrator module to use
with the ``set backend`` command::

    ceph orch set backend <module>

For example, to enable the Rook orchestrator module and use it with the CLI::

    ceph mgr module enable rook
    ceph orch set backend rook

Check the backend is properly configured::

    ceph orch status

Disable the Orchestrator
------------------------

To disable the orchestrator, use the empty string ``""``::

    ceph orch set backend ""
    ceph mgr module disable rook

Current Implementation Status
=============================

This is an overview of the current implementation status of the orchestrators.

=================================== ====== =========
 Command                             Rook   Cephadm
=================================== ====== =========
 apply iscsi                         ⚪      ⚪
 apply mds                           ✔      ✔
 apply mgr                           ⚪      ✔
 apply mon                           ✔      ✔
 apply nfs                           ✔      ⚪
 apply osd                           ✔      ✔
 apply rbd-mirror                    ✔      ✔
 apply rgw                           ⚪      ✔
 host add                            ⚪      ✔
 host ls                             ✔      ✔
 host rm                             ⚪      ✔
 daemon status                       ⚪      ✔
 daemon {stop,start,...}             ⚪      ✔
 device {ident,fault}-(on,off}       ⚪      ✔
 device ls                           ✔      ✔
 iscsi add                           ⚪      ⚪
 mds add                             ✔      ✔
 nfs add                             ✔      ⚪
 ps                                  ⚪      ✔
 rbd-mirror add                      ⚪      ✔
 rgw add                             ✔      ✔
 ps                                  ✔      ✔
=================================== ====== =========

where

* ⚪ = not yet implemented
* ❌ = not applicable
* ✔ = implemented
