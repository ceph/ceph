
.. _orchestrator-cli-module:

================
Orchestrator CLI
================

This module provides a command line interface (CLI) for orchestrator modules.
The commands are carried out by an orchestrator backend, a ``ceph-mgr`` module
such as cephadm, which manages hosts directly over SSH, or Rook, which works
through Kubernetes.

Definition of Terms
===================

The orchestrator CLI works with more than one backend, so it uses a common set
of terms:

+--------------------------------------+---------------------------------------+
| *host*                               | The name of the host as the           |
|                                      | orchestrator knows it. With cephadm,  |
|                                      | this is the output of ``hostname`` on |
|                                      | that host, which can be a bare host   |
|                                      | name or a fully qualified domain name |
|                                      | (see :ref:`cephadm-fqdn`). With Rook, |
|                                      | it is the Kubernetes node name. Not   |
|                                      | the pod name, container name, or host |
|                                      | name inside a container.              |
+--------------------------------------+---------------------------------------+
| *service type*                       | The type of the service. e.g., nfs,   |
|                                      | mds, osd, mon, rgw, mgr, iscsi        |
+--------------------------------------+---------------------------------------+
| *service*                            | A logical service. Typically          |
|                                      | comprised of multiple service         |
|                                      | instances on multiple hosts for HA    |
|                                      |                                       |
|                                      | * the file system name for the        |
|                                      |   ``mds`` type                        |
|                                      | * a service ID that you choose for    |
|                                      |   the ``rgw`` type (the realm, zone   |
|                                      |   group, and zone are separate        |
|                                      |   settings)                           |
|                                      | * the NFS cluster ID for the ``nfs``  |
|                                      |   type                                |
+--------------------------------------+---------------------------------------+
| *daemon*                             | A single instance of a service on one |
|                                      | host, for example one ``ceph-mds``    |
|                                      | process.                              |
|                                      |                                       |
|                                      | This identifier should                |
|                                      | uniquely identify the instance.       |
+--------------------------------------+---------------------------------------+

Here is how the names relate: 

* A *service* has a specific *service type*.
* A *daemon* is a physical instance of a *service type*.

.. note::

    Orchestrator modules might implement only a subset of the commands listed
    below. The implementation of the commands may differ between modules.

Status
======

.. prompt:: bash #

   ceph orch status [--detail]

This command shows the current orchestrator mode and its high-level status
(whether the orchestrator plugin is available and operational).


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

.. note::

   With cephadm, the orchestrator deploys and places the daemons and applies
   the settings in the service specification. It does not create what a
   service serves, such as a CephFS file system, an RGW realm and zone, or NFS
   exports. See the relevant documentation for details about how to configure
   particular services.

The ``name`` parameter identifies the kind of the group of instances. The
following short list explains the meaning of the ``name`` parameter:

* A CephFS file system identifies a group of MDS daemons.
* For RGW, ``name`` is a service ID that you choose. Set the realm and the
  zone with ``--realm`` and ``--zone``; use both options or neither.

Creating/growing/shrinking/removing services:

.. prompt:: bash #

   ceph orch apply mds <fs_name> [--placement=<placement>] [--dry-run]
   ceph orch apply rgw <name> [--realm=<realm>] [--zonegroup=<zonegroup>] [--zone=<zone>] [--port=<port>] [--ssl] [--placement=<placement>] [--dry-run]
   ceph orch apply nfs <name> [--placement=<placement>] [--port=<port>] [--dry-run]
   ceph orch rm <service_name> [--force] [--force-delete-data]

where ``placement`` is a :ref:`orchestrator-cli-placement-spec`.

An NFS service keeps its configuration in the ``.nfs`` pool, in a namespace
named after the NFS cluster ID (the service ID).

For ``ceph orch rm``, ``--force`` may be required in some cases (for example when
removing an OSD service that would leave OSDs behind).

The ``--force-delete-data`` flag requires that ``--force`` is also passed.
This directs cephadm to delete on-disk data for certain daemon types instead
of moving it to ``<fsid>/removed/`` on the host. These daemon types include
``mon``, ``osd``, and ``prometheus``. This helps avoid filling up the
underlying filesystem over time.

e.g., ``ceph orch apply mds myfs --placement="3 host1 host2 host3"``

Service Commands:

.. prompt:: bash #

   ceph orch <start|stop|restart|redeploy|reconfig> <service_name>

.. note:: These commands apply only to cephadm containerized daemons.

Options
=======

.. option:: start

   Start every daemon of the service.

.. option:: stop

   Stop every daemon of the service. Cephadm refuses to stop the whole
   ``mgr``, ``mon``, or ``osd`` service.

.. option:: restart

   Restart every daemon of the service.

.. option:: redeploy

   Redeploy every daemon of the service. For each daemon, this recreates
   the daemon directory ``/var/lib/ceph/<fsid>/<daemon-name>`` if it does not
   exist, refreshes its configuration files, regenerates its systemd unit
   files, and restarts it.

.. option:: reconfig

   Reconfigure the daemons of the service. This refreshes their configuration
   files. Ceph daemons (``mon``, ``mgr``, ``osd``, ``mds``, ``rgw``,
   ``rbd-mirror``, ``crash``, ``cephfs-mirror``, and ``ceph-exporter``) are not
   restarted. Other daemons, such as Prometheus, Grafana, Alertmanager, or NFS,
   are restarted so that they read the new files.

   .. note:: this command assumes the daemon directory ``/var/lib/ceph/<fsid>/<daemon-name>`` already exists.

To act on a single daemon, use ``ceph orch daemon <action> <daemon-name>``.


Configuring the Orchestrator CLI
================================

Enable the orchestrator by using the ``set backend`` command to select the orchestrator module that will be used:

.. prompt:: bash #

   ceph orch set backend <module>

Example - Configuring the Orchestrator CLI
------------------------------------------

For example, to enable the Rook orchestrator module and use it with the CLI:

.. prompt:: bash #

   ceph mgr module enable rook
   ceph orch set backend rook

Confirm that the backend is properly configured:

.. prompt:: bash #

   ceph orch status

Disable the Orchestrator
------------------------

To disable the orchestrator, use the empty string ``""``:

.. prompt:: bash #

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
 apply -i <container spec>           ⚪      ✔
 apply snmp-gateway                  ❌      ✔
 host add                            ⚪      ✔
 host ls                             ✔      ✔
 host rm                             ⚪      ✔
 host maintenance enter              ❌      ✔
 host maintenance exit               ❌      ✔
 daemon {stop,start,...}             ⚪      ✔
 device light                        ⚪      ✔
 device ls                           ✔      ✔
 daemon add iscsi                    ⚪      ✔
 daemon add mds                      ⚪      ✔
 daemon add nfs                      ⚪      ✔
 daemon add rbd-mirror               ⚪      ✔
 daemon add rgw                      ⚪      ✔
 ls                                  ✔      ✔
 ps                                  ✔      ✔
 status                              ✔      ✔
 upgrade                             ❌      ✔
=================================== ====== =========

where

* ⚪ = not yet implemented
* ❌ = not applicable
* ✔ = implemented

The ``device light`` row is the ``ceph device light`` command, which has no
``orch`` prefix.
