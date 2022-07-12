
.. _orchestrator-cli-module:

================
Orchestrator CLI
================

This module provides a command line interface (CLI) to orchestrator
modules (``ceph-mgr`` modules which interface with external orchestration services).

As the orchestrator CLI unifies multiple external orchestrators, a common nomenclature
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

* A *service* has a specific *service type*
* A *daemon* is a physical instance of a *service type*


.. note::

    Orchestrator modules may only implement a subset of the commands listed below.
    Also, the implementation of the commands may differ between modules.

Status
======

::

    ceph orch status [--detail]

Show current orchestrator mode and high-level status (whether the orchestrator
plugin is available and operational)


..
    Turn On Device Lights
    ^^^^^^^^^^^^^^^^^^^^^
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

    where ``journal`` is the filestore journal device, ``wal`` is the bluestore
    write ahead log device, and ``all`` stands for all devices associated with the OSD


.. _orchestrator-cli-stateless-services:

Stateless services (MDS/RGW/NFS/rbd-mirror/iSCSI)
=================================================

(Please note: The orchestrator will not configure the services. Please look into the corresponding
documentation for service configuration details.)

The ``name`` parameter is an identifier of the group of instances:

* a CephFS file system for a group of MDS daemons,
* a zone name for a group of RGWs

Creating/growing/shrinking/removing services::

    ceph orch apply mds <fs_name> [--placement=<placement>] [--dry-run]
    ceph orch apply rgw <name> [--realm=<realm>] [--zone=<zone>] [--port=<port>] [--ssl] [--placement=<placement>] [--dry-run]
    ceph orch apply nfs <name> <pool> [--namespace=<namespace>] [--placement=<placement>] [--dry-run]
    ceph orch rm <service_name> [--force]

where ``placement`` is a :ref:`orchestrator-cli-placement-spec`.

e.g., ``ceph orch apply mds myfs --placement="3 host1 host2 host3"``

Service Commands::

    ceph orch <start|stop|restart|redeploy|reconfig> <service_name>



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
 apply iscsi                         ⚪     ✔
 apply mds                           ✔      ✔
 apply mgr                           ⚪      ✔
 apply mon                           ✔      ✔
 apply nfs                           ✔      ✔
 apply osd                           ✔      ✔
 apply rbd-mirror                    ✔      ✔
 apply cephfs-mirror                 ⚪      ✔
 apply grafana                       ⚪      ✔
 apply prometheus                    ❌      ✔
 apply alertmanager                  ❌      ✔
 apply node-exporter                 ❌      ✔
 apply rgw                           ✔       ✔
 apply container                     ⚪      ✔
 apply snmp-gateway                  ❌      ✔
 host add                            ⚪      ✔
 host ls                             ✔      ✔
 host rm                             ⚪      ✔
 host maintenance enter              ❌      ✔
 host maintenance exit               ❌      ✔
 daemon status                       ⚪      ✔
 daemon {stop,start,...}             ⚪      ✔
 device {ident,fault}-(on,off}       ⚪      ✔
 device ls                           ✔      ✔
 iscsi add                           ⚪     ✔
 mds add                             ⚪      ✔
 nfs add                             ⚪      ✔
 rbd-mirror add                      ⚪      ✔
 rgw add                             ⚪     ✔
 ls                                  ✔      ✔
 ps                                  ✔      ✔
 status                              ✔      ✔
 upgrade                             ❌      ✔
=================================== ====== =========

where

* ⚪ = not yet implemented
* ❌ = not applicable
* ✔ = implemented
