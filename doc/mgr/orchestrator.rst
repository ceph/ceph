
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

    ceph orch host add <hostname> [<addr>] [<labels>...]
    ceph orch host rm <hostname>

For cephadm, see also :ref:`cephadm-fqdn`.

Host Specification
------------------

Many hosts can be added at once using
``ceph orch apply -i`` by submitting a multi-document YAML file::

    ---
    service_type: host
    addr: node-00
    hostname: node-00
    labels:
    - example1
    - example2
    ---
    service_type: host
    addr: node-01
    hostname: node-01
    labels:
    - grafana
    ---
    service_type: host
    addr: node-02
    hostname: node-02

This can be combined with service specifications (below) to create a cluster spec file to deploy a whole cluster in one command.  see ``cephadm bootstrap --apply-spec`` also to do this during bootstrap. Cluster SSH Keys must be copied to hosts prior.

OSD Management
==============

List Devices
------------

Print a list of discovered devices, grouped by host and optionally
filtered to a particular host:

::

    ceph orch device ls [--host=...] [--refresh]

Example::

    HOST    PATH      TYPE   SIZE  DEVICE  AVAIL  REJECT REASONS
    master  /dev/vda  hdd   42.0G          False  locked
    node1   /dev/vda  hdd   42.0G          False  locked
    node1   /dev/vdb  hdd   8192M  387836  False  locked, LVM detected, Insufficient space (<5GB) on vgs
    node1   /dev/vdc  hdd   8192M  450575  False  locked, LVM detected, Insufficient space (<5GB) on vgs
    node3   /dev/vda  hdd   42.0G          False  locked
    node3   /dev/vdb  hdd   8192M  395145  False  LVM detected, locked, Insufficient space (<5GB) on vgs
    node3   /dev/vdc  hdd   8192M  165562  False  LVM detected, locked, Insufficient space (<5GB) on vgs
    node2   /dev/vda  hdd   42.0G          False  locked
    node2   /dev/vdb  hdd   8192M  672147  False  LVM detected, Insufficient space (<5GB) on vgs, locked
    node2   /dev/vdc  hdd   8192M  228094  False  LVM detected, Insufficient space (<5GB) on vgs, locked




Erase Devices (Zap Devices)
---------------------------

Erase (zap) a device so that it can be resued. ``zap`` calls ``ceph-volume zap`` on the remote host.

::

     orch device zap <hostname> <path>

Example command::

     ceph orch device zap my_hostname /dev/sdx

.. note::
    Cephadm orchestrator will automatically deploy drives that match the DriveGroup in your OSDSpec if the unmanaged flag is unset.
    For example, if you use the ``all-available-devices`` option when creating OSD's, when you ``zap`` a device the cephadm orchestrator will automatically create a new OSD in the device .
    To disable this behavior, see :ref:`orchestrator-cli-create-osds`.

.. _orchestrator-cli-create-osds:

Create OSDs
-----------

Create OSDs on a group of devices on a single host::

    ceph orch daemon add osd <host>:device1,device2

Example::

    # ceph orch daemon add osd node1:/dev/vdd
    Created 1 OSD on host 'node1' using device '/dev/vdd'

or::

    ceph orch apply osd -i <json_file/yaml_file>

Where the ``json_file/yaml_file`` is a DriveGroup specification.
For a more in-depth guide to DriveGroups please refer to :ref:`drivegroups`

or::

    ceph orch apply osd --all-available-devices

If the 'apply' method is used. You will be presented with a preview of what will happen.

Example::

    # ceph orch apply osd --all-available-devices
    NAME                  HOST  DATA     DB WAL
    all-available-devices node1 /dev/vdb -  -
    all-available-devices node2 /dev/vdc -  -
    all-available-devices node3 /dev/vdd -  -

.. note::
    Example output from cephadm orchestrator

When the parameter ``all-available-devices`` or a DriveGroup specification is used, a cephadm service is created.
This service guarantees that all available devices or devices included in the DriveGroup will be used for OSD's.
Take into account the implications of this behavior, which is automatic and enabled by default.

For example:

After using::

    ceph orch apply osd --all-available-devices

* If you add new disks to the cluster they will automatically be used to create new OSD's.
* A new OSD will be created automatically if you remove an OSD and clean the LVM physical volume.

If you want to avoid this behavior (disable automatic creation of OSD in available devices), use the ``unmanaged`` parameter::

    ceph orch apply osd --all-available-devices --unmanaged=true

In the case that you have already created the OSD's using the ``all-available-devices`` service, you can change the automatic OSD creation using the following command::

    ceph orch osd spec --service-name  osd.all-available-devices --unmanaged

Remove an OSD
-------------------
::

    ceph orch osd rm <svc_id>... [--replace] [--force]

Removes one or more OSDs from the cluster.

Example::

    # ceph orch osd rm 4
    Scheduled OSD(s) for removal


OSDs that are not safe-to-destroy will be rejected.

You can query the state of the operation with::

    # ceph orch osd rm status
    NAME  HOST  PGS STARTED_AT
    osd.7 node1 55 2020-04-22 19:28:38.785761
    osd.5 node3 3 2020-04-22 19:28:34.201685
    osd.3 node2 0 2020-04-22 19:28:34.201695


When no PGs are left on the osd, it will be decommissioned and removed from the cluster.

.. note::
    After removing an OSD, if you wipe the LVM physical volume in the device used by the removed OSD, a new OSD will be created.
    Read information about the ``unmanaged`` parameter in :ref:`orchestrator-cli-create-osds`.

Replace an OSD
-------------------
::

    orch osd rm <svc_id>... --replace [--force]

Example::

    # ceph orch osd rm 4 --replace
    Scheduled OSD(s) for replacement


This follows the same procedure as the "Remove OSD" part with the exception that the OSD is not permanently removed
from the crush hierarchy, but is assigned a 'destroyed' flag.

**Preserving the OSD ID**

The previously set the 'destroyed' flag is used to determined osd ids that will be reused in the next osd deployment.

If you use OSDSpecs for osd deployment, your newly added disks will be assigned with the osd ids of their replaced
counterpart, granted the new disk still match the OSDSpecs.

For assistance in this process you can use the 'preview' feature:

Example::


    ceph orch apply osd --service-name <name_of_osd_spec> --preview
    NAME                  HOST  DATA     DB WAL
    <name_of_osd_spec>    node1 /dev/vdb -  -

Tip: The name of your OSDSpec can be retrieved from **ceph orch ls**

Alternatively, you can use your OSDSpec file::

    ceph orch apply osd -i <osd_spec_file> --preview
    NAME                  HOST  DATA     DB WAL
    <name_of_osd_spec>    node1 /dev/vdb -  -


If this matches your anticipated behavior, just omit the --preview flag to execute the deployment.


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

    ceph orch ls [--service_type type] [--service_name name] [--export] [--format f] [--refresh]

Discover the status of a particular service or daemons::

    ceph orch ls --service_type type --service_name <name> [--refresh]

Export the service specs known to the orchestrator as yaml in format
that is compatible to ``ceph orch apply -i``::

    ceph orch ls --export


Daemon Status
=============

Print a list of all daemons known to the orchestrator::

    ceph orch ps [--hostname host] [--daemon_type type] [--service_name name] [--daemon_id id] [--format f] [--refresh]

Query the status of a particular service instance (mon, osd, mds, rgw).  For OSDs
the id is the numeric OSD ID, for MDS services it is the file system name::

    ceph orch ps --daemon_type osd --daemon_id 0


.. _orchestrator-cli-cephfs:

Depoying CephFS
===============

In order to set up a :term:`CephFS`, execute::

    ceph fs volume create <fs_name> <placement spec>

Where ``name`` is the name of the CephFS, ``placement`` is a
:ref:`orchestrator-cli-placement-spec`.

This command will create the required Ceph pools, create the new
CephFS, and deploy mds servers.

Stateless services (MDS/RGW/NFS/rbd-mirror/iSCSI)
=================================================

The orchestrator is not responsible for configuring the services. Please look into the corresponding
documentation for details.

The ``name`` parameter is an identifier of the group of instances:

* a CephFS file system for a group of MDS daemons,
* a zone name for a group of RGWs

Creating/growing/shrinking/removing services::

    ceph orch apply mds <fs_name> [--placement=<placement>]
    ceph orch apply rgw <realm> <zone> [--subcluster=<subcluster>] [--port=<port>] [--ssl] [--placement=<placement>]
    ceph orch apply nfs <name> <pool> [--namespace=<namespace>] [--placement=<placement>]
    ceph orch rm <service_name> [--force]

Where ``placement`` is a :ref:`orchestrator-cli-placement-spec`.

e.g., ``ceph orch apply mds myfs --placement="3 host1 host2 host3"``

Service Commands::

    ceph orch <start|stop|restart|redeploy|reconfig> <service_name>

.. _orchestrator-cli-service-spec:

Service Specification
=====================

As *Service Specification* is a data structure often represented as YAML
to specify the deployment of services. For example:

.. code-block:: yaml

    service_type: rgw
    service_id: realm.zone
    placement:
      hosts:
        - host1
        - host2
        - host3
    spec: ...
    unmanaged: false

Where the properties of a service specification are the following:

* ``service_type`` is the type of the service. Needs to be either a Ceph
   service (``mon``, ``crash``, ``mds``, ``mgr``, ``osd`` or
   ``rbd-mirror``), a gateway (``nfs`` or ``rgw``), or part of the
   monitoring stack (``alertmanager``, ``grafana``, ``node-exporter`` or
   ``prometheus``).
* ``service_id`` is the name of the service. Omit the service time
* ``placement`` is a :ref:`orchestrator-cli-placement-spec`
* ``spec``: additional specifications for a specific service.
* ``unmanaged``: If set to ``true``, the orchestrator will not deploy nor
   remove any daemon associated with this service. Placement and all other
   properties will be ignored. This is useful, if this service should not
   be managed temporarily.

Each service type can have different requirements for the spec.

Service specifications of type ``mon``, ``mgr``, and the monitoring
types do not require a ``service_id``

A service of type ``nfs`` requires a pool name and contain
an optional namespace:

.. code-block:: yaml

    service_type: nfs
    service_id: mynfs
    placement:
      hosts:
        - host1
        - host2
    spec:
      pool: mypool
      namespace: mynamespace

Where ``pool`` is a RADOS pool where NFS client recovery data is stored
and ``namespace`` is a RADOS namespace where NFS client recovery
data is stored in the pool.

A service of type ``osd`` is in detail described in :ref:`drivegroups`

Many service specifications can then be applied at once using
``ceph orch apply -i`` by submitting a multi-document YAML file::

    cat <<EOF | ceph orch apply -i -
    service_type: mon
    placement:
      host_pattern: "mon*"
    ---
    service_type: mgr
    placement:
      host_pattern: "mgr*"
    ---
    service_type: osd
    placement:
      host_pattern: "osd*"
    data_devices:
      all: true
    EOF

.. _orchestrator-cli-placement-spec:

Placement Specification
=======================

In order to allow the orchestrator to deploy a *service*, it needs to
know how many and where it should deploy *daemons*. The orchestrator
defines a placement specification that can either be passed as a command line argument.

Explicit placements
-------------------

Daemons can be explictly placed on hosts by simply specifying them::

    orch apply prometheus "host1 host2 host3"

Or in yaml:

.. code-block:: yaml

    service_type: prometheus
    placement:
      hosts:
        - host1
        - host2
        - host3

MONs and other services may require some enhanced network specifications::

  orch daemon add mon myhost:[v2:1.2.3.4:3000,v1:1.2.3.4:6789]=name

Where ``[v2:1.2.3.4:3000,v1:1.2.3.4:6789]`` is the network address of the monitor
and ``=name`` specifies the name of the new monitor.

Placement by labels
-------------------

Daemons can be explictly placed on hosts that match a specifc label::

    orch apply prometheus label:mylabel

Or in yaml:

.. code-block:: yaml

    service_type: prometheus
    placement:
      label: "mylabel"


Placement by pattern matching
-----------------------------

Daemons can be placed on hosts as well::

    orch apply prometheus 'myhost[1-3]'

Or in yaml:

.. code-block:: yaml

    service_type: prometheus
    placement:
      host_pattern: "myhost[1-3]"

To place a service on *all* hosts, use ``"*"``::

    orch apply crash '*'

Or in yaml:

.. code-block:: yaml

    service_type: node-exporter
    placement:
      host_pattern: "*"


Setting a limit
---------------

By specifying ``count``, only that number of daemons will be created::

    orch apply prometheus 3

To deploy *daemons* on a subset of hosts, also specify the count::

    orch apply prometheus "2 host1 host2 host3"

If the count is bigger than the amount of hosts, cephadm still deploys two daemons::

    orch apply prometheus "3 host1 host2"

Or in yaml:

.. code-block:: yaml

    service_type: prometheus
    placement:
      count: 3

Or with hosts:

.. code-block:: yaml

    service_type: prometheus
    placement:
      count: 2
      hosts:
        - host1
        - host2
        - host3

Updating Service Specifications
===============================

The Ceph Orchestrator maintains a declarative state of each
service in a ``ServiceSpec``. For certain operations, like updating
the RGW HTTP port, we need to update the existing
specification.

1. List the current ``ServiceSpec``::

    ceph orch ls --service_name=<service-name> --export > myservice.yaml

2. Update the yaml file::

    vi myservice.yaml

3. Apply the new ``ServiceSpec``::

    ceph orch apply -i myservice.yaml

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
 apply rgw                           ⚪      ✔
 host add                            ⚪      ✔
 host ls                             ✔      ✔
 host rm                             ⚪      ✔
 daemon status                       ⚪      ✔
 daemon {stop,start,...}             ⚪      ✔
 device {ident,fault}-(on,off}       ⚪      ✔
 device ls                           ✔      ✔
 iscsi add                           ⚪     ✔
 mds add                             ✔      ✔
 nfs add                             ✔      ✔
 rbd-mirror add                      ⚪      ✔
 rgw add                             ✔      ✔
 ps                                  ✔      ✔
=================================== ====== =========

where

* ⚪ = not yet implemented
* ❌ = not applicable
* ✔ = implemented
