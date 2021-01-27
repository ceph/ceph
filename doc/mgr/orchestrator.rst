
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

    ceph orch status

Show current orchestrator mode and high-level status (whether the orchestrator
plugin is available and operational)

.. _orchestrator-cli-host-management:

Host Management
===============

List hosts associated with the cluster::

    ceph orch host ls

Add and remove hosts::

    ceph orch host add <hostname> [<addr>] [<labels>...]
    ceph orch host rm <hostname>

For cephadm, see also :ref:`cephadm-fqdn` and :ref:`cephadm-removing-hosts`.

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

This can be combined with service specifications (below) to create a cluster spec file to deploy a whole cluster in one command.  see ``cephadm bootstrap --apply-spec`` also to do this during bootstrap. Cluster SSH Keys must be copied to hosts prior to adding them.

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

Erase (zap) a device so that it can be reused. ``zap`` calls ``ceph-volume zap`` on the remote host.

::

     orch device zap <hostname> <path>

Example command::

     ceph orch device zap my_hostname /dev/sdx

.. note::
    Cephadm orchestrator will automatically deploy drives that match the DriveGroup in your OSDSpec if the unmanaged flag is unset.
    For example, if you use the ``all-available-devices`` option when creating OSDs, when you ``zap`` a device the cephadm orchestrator will automatically create a new OSD in the device .
    To disable this behavior, see :ref:`orchestrator-cli-create-osds`.

.. _orchestrator-cli-create-osds:

Create OSDs
-----------

Create OSDs on a set of devices on a single host::

    ceph orch daemon add osd <host>:device1,device2

Another way of doing it is using ``apply`` interface::

    ceph orch apply osd -i <json_file/yaml_file> [--dry-run]

where the ``json_file/yaml_file`` is a DriveGroup specification.
For a more in-depth guide to DriveGroups please refer to :ref:`drivegroups`

``dry-run`` will cause the orchestrator to present a preview of what will happen
without actually creating the OSDs.

Example::

    # ceph orch apply osd --all-available-devices --dry-run
    NAME                  HOST  DATA     DB WAL
    all-available-devices node1 /dev/vdb -  -
    all-available-devices node2 /dev/vdc -  -
    all-available-devices node3 /dev/vdd -  -

When the parameter ``all-available-devices`` or a DriveGroup specification is used, a cephadm service is created.
This service guarantees that all available devices or devices included in the DriveGroup will be used for OSDs.
Note that the effect of ``--all-available-devices`` is persistent; that is, drives which are added to the system 
or become available (say, by zapping) after the command is complete will be automatically found and added to the cluster.

That is, after using::

    ceph orch apply osd --all-available-devices

* If you add new disks to the cluster they will automatically be used to create new OSDs.
* A new OSD will be created automatically if you remove an OSD and clean the LVM physical volume.

If you want to avoid this behavior (disable automatic creation of OSD on available devices), use the ``unmanaged`` parameter::

    ceph orch apply osd --all-available-devices --unmanaged=true

Remove an OSD
-------------
::

    ceph orch osd rm <osd_id(s)> [--replace] [--force]

Evacuates PGs from an OSD and removes it from the cluster.

Example::

    # ceph orch osd rm 0
    Scheduled OSD(s) for removal


OSDs that are not safe-to-destroy will be rejected.

You can query the state of the operation with::

    # ceph orch osd rm status
    OSD_ID  HOST         STATE                    PG_COUNT  REPLACE  FORCE  STARTED_AT
    2       cephadm-dev  done, waiting for purge  0         True     False  2020-07-17 13:01:43.147684
    3       cephadm-dev  draining                 17        False    True   2020-07-17 13:01:45.162158
    4       cephadm-dev  started                  42        False    True   2020-07-17 13:01:45.162158


When no PGs are left on the OSD, it will be decommissioned and removed from the cluster.

.. note::
    After removing an OSD, if you wipe the LVM physical volume in the device used by the removed OSD, a new OSD will be created.
    Read information about the ``unmanaged`` parameter in :ref:`orchestrator-cli-create-osds`.

Stopping OSD Removal
--------------------

You can stop the queued OSD removal operation with

::

    ceph orch osd rm stop <svc_id(s)>

Example::

    # ceph orch osd rm stop 4
    Stopped OSD(s) removal

This will reset the initial state of the OSD and take it off the removal queue.


Replace an OSD
-------------------
::

    orch osd rm <svc_id(s)> --replace [--force]

Example::

    # ceph orch osd rm 4 --replace
    Scheduled OSD(s) for replacement


This follows the same procedure as the "Remove OSD" part with the exception that the OSD is not permanently removed
from the CRUSH hierarchy, but is assigned a 'destroyed' flag.

**Preserving the OSD ID**

The previously-set 'destroyed' flag is used to determine OSD ids that will be reused in the next OSD deployment.

If you use OSDSpecs for OSD deployment, your newly added disks will be assigned the OSD ids of their replaced
counterparts, assuming the new disks still match the OSDSpecs.

For assistance in this process you can use the '--dry-run' feature.

Tip: The name of your OSDSpec can be retrieved from **ceph orch ls**

Alternatively, you can use your OSDSpec file::

    ceph orch apply osd -i <osd_spec_file> --dry-run
    NAME                  HOST  DATA     DB WAL
    <name_of_osd_spec>    node1 /dev/vdb -  -


If this matches your anticipated behavior, just omit the --dry-run flag to execute the deployment.


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


Monitor and manager management
==============================

Creates or removes MONs or MGRs from the cluster. Orchestrator may return an
error if it doesn't know how to do this transition.

Update the number of monitor hosts::

    ceph orch apply mon --placement=<placement> [--dry-run]
    
Where ``placement`` is a :ref:`orchestrator-cli-placement-spec`.

Each host can optionally specify a network for the monitor to listen on.

Update the number of manager hosts::

    ceph orch apply mgr --placement=<placement> [--dry-run]
    
Where ``placement`` is a :ref:`orchestrator-cli-placement-spec`.

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

For examples about retrieving specs of single services see :ref:`orchestrator-cli-service-spec-retrieve`.

Daemon Status
=============

Print a list of all daemons known to the orchestrator::

    ceph orch ps [--hostname host] [--daemon_type type] [--service_name name] [--daemon_id id] [--format f] [--refresh]

Query the status of a particular service instance (mon, osd, mds, rgw).  For OSDs
the id is the numeric OSD ID, for MDS services it is the file system name::

    ceph orch ps --daemon_type osd --daemon_id 0


.. _orchestrator-cli-cephfs:

Deploying CephFS
================

In order to set up a :term:`CephFS`, execute::

    ceph fs volume create <fs_name> <placement spec>

where ``name`` is the name of the CephFS and ``placement`` is a
:ref:`orchestrator-cli-placement-spec`.

This command will create the required Ceph pools, create the new
CephFS, and deploy mds servers.


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
    ceph orch apply rgw <realm> <zone> [--subcluster=<subcluster>] [--port=<port>] [--ssl] [--placement=<placement>] [--dry-run]
    ceph orch apply nfs <name> <pool> [--namespace=<namespace>] [--placement=<placement>] [--dry-run]
    ceph orch rm <service_name> [--force]

where ``placement`` is a :ref:`orchestrator-cli-placement-spec`.

e.g., ``ceph orch apply mds myfs --placement="3 host1 host2 host3"``

Service Commands::

    ceph orch <start|stop|restart|redeploy|reconfig> <service_name>

Deploying custom containers
===========================

The orchestrator enables custom containers to be deployed using a YAML file.
A corresponding :ref:`orchestrator-cli-service-spec` must look like:

.. code-block:: yaml

    service_type: container
    service_id: foo
    placement:
        ...
    image: docker.io/library/foo:latest
    entrypoint: /usr/bin/foo
    uid: 1000
    gid: 1000
    args:
        - "--net=host"
        - "--cpus=2"
    ports:
        - 8080
        - 8443
    envs:
        - SECRET=mypassword
        - PORT=8080
        - PUID=1000
        - PGID=1000
    volume_mounts:
        CONFIG_DIR: /etc/foo
    bind_mounts:
      - ['type=bind', 'source=lib/modules', 'destination=/lib/modules', 'ro=true']
    dirs:
      - CONFIG_DIR
    files:
      CONFIG_DIR/foo.conf:
        - refresh=true
        - username=xyz
        - "port: 1234"

where the properties of a service specification are:

* ``service_id``
    A unique name of the service.
* ``image``
    The name of the Docker image.
* ``uid``
    The UID to use when creating directories and files in the host system.
* ``gid``
    The GID to use when creating directories and files in the host system.
* ``entrypoint``
    Overwrite the default ENTRYPOINT of the image.
* ``args``
    A list of additional Podman/Docker command line arguments.
* ``ports``
    A list of TCP ports to open in the host firewall.
* ``envs``
    A list of environment variables.
* ``bind_mounts``
    When you use a bind mount, a file or directory on the host machine
    is mounted into the container. Relative `source=...` paths will be
    located below `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.
* ``volume_mounts``
    When you use a volume mount, a new directory is created within
    Docker’s storage directory on the host machine, and Docker manages
    that directory’s contents. Relative source paths will be located below
    `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.
* ``dirs``
    A list of directories that are created below
    `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.
* ``files``
    A dictionary, where the key is the relative path of the file and the
    value the file content. The content must be double quoted when using
    a string. Use '\\n' for line breaks in that case. Otherwise define
    multi-line content as list of strings. The given files will be created
    below the directory `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.
    The absolute path of the directory where the file will be created must
    exist. Use the `dirs` property to create them if necessary.

.. _orchestrator-cli-service-spec:

Service Specification
=====================

A *Service Specification* is a data structure represented as YAML
to specify the deployment of services.  For example:

.. code-block:: yaml

    service_type: rgw
    service_id: realm.zone
    placement:
      hosts:
        - host1
        - host2
        - host3
    unmanaged: false
    ...

where the properties of a service specification are:

* ``service_type``
    The type of the service. Needs to be either a Ceph
    service (``mon``, ``crash``, ``mds``, ``mgr``, ``osd`` or
    ``rbd-mirror``), a gateway (``nfs`` or ``rgw``), part of the
    monitoring stack (``alertmanager``, ``grafana``, ``node-exporter`` or
    ``prometheus``) or (``container``) for custom containers.
* ``service_id``
    The name of the service.
* ``placement``
    See :ref:`orchestrator-cli-placement-spec`.
* ``unmanaged``
    If set to ``true``, the orchestrator will not deploy nor
    remove any daemon associated with this service. Placement and all other
    properties will be ignored. This is useful, if this service should not
    be managed temporarily.

Each service type can have additional service specific properties.

Service specifications of type ``mon``, ``mgr``, and the monitoring
types do not require a ``service_id``.

A service of type ``nfs`` requires a pool name and may contain
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

where ``pool`` is a RADOS pool where NFS client recovery data is stored
and ``namespace`` is a RADOS namespace where NFS client recovery
data is stored in the pool.

A service of type ``osd`` is described in :ref:`drivegroups`

Many service specifications can be applied at once using
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
    service_id: default_drive_group
    placement:
      host_pattern: "osd*"
    data_devices:
      all: true
    EOF

.. _orchestrator-cli-service-spec-retrieve:

Retrieving the running Service Specification
--------------------------------------------

If the services have been started via ``ceph orch apply...``, then directly changing
the Services Specification is complicated. Instead of attempting to directly change
the Services Specification, we suggest exporting the running Service Specification by
following these instructions::
    
    ceph orch ls --service-name rgw.<realm>.<zone> --export > rgw.<realm>.<zone>.yaml
    ceph orch ls --service-type mgr --export > mgr.yaml
    ceph orch ls --export > cluster.yaml

The Specification can then be changed and re-applied as above.

.. _orchestrator-cli-placement-spec:

Placement Specification
=======================

For the orchestrator to deploy a *service*, it needs to know where to deploy
*daemons*, and how many to deploy.  This is the role of a placement
specification.  Placement specifications can either be passed as command line arguments
or in a YAML files.

Explicit placements
-------------------

Daemons can be explicitly placed on hosts by simply specifying them::

    orch apply prometheus --placement="host1 host2 host3"

Or in YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      hosts:
        - host1
        - host2
        - host3

MONs and other services may require some enhanced network specifications::

  orch daemon add mon --placement="myhost:[v2:1.2.3.4:3300,v1:1.2.3.4:6789]=name"

where ``[v2:1.2.3.4:3300,v1:1.2.3.4:6789]`` is the network address of the monitor
and ``=name`` specifies the name of the new monitor.

Placement by labels
-------------------

Daemons can be explicitly placed on hosts that match a specific label::

    orch apply prometheus --placement="label:mylabel"

Or in YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      label: "mylabel"


Placement by pattern matching
-----------------------------

Daemons can be placed on hosts as well::

    orch apply prometheus --placement='myhost[1-3]'

Or in YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      host_pattern: "myhost[1-3]"

To place a service on *all* hosts, use ``"*"``::

    orch apply crash --placement='*'

Or in YAML:

.. code-block:: yaml

    service_type: node-exporter
    placement:
      host_pattern: "*"


Setting a limit
---------------

By specifying ``count``, only that number of daemons will be created::

    orch apply prometheus --placement=3

To deploy *daemons* on a subset of hosts, also specify the count::

    orch apply prometheus --placement="2 host1 host2 host3"

If the count is bigger than the amount of hosts, cephadm deploys one per host::

    orch apply prometheus --placement="3 host1 host2"

results in two Prometheus daemons.

Or in YAML:

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

    ceph orch apply -i myservice.yaml [--dry-run]

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
 apply container                     ⚪      ✔
 host add                            ⚪      ✔
 host ls                             ✔      ✔
 host rm                             ⚪      ✔
 daemon status                       ⚪      ✔
 daemon {stop,start,...}             ⚪      ✔
 device {ident,fault}-(on,off}       ⚪      ✔
 device ls                           ✔      ✔
 iscsi add                           ⚪     ✔
 mds add                             ⚪      ✔
 nfs add                             ⚪      ✔
 rbd-mirror add                      ⚪      ✔
 rgw add                             ⚪     ✔
 ps                                  ✔      ✔
=================================== ====== =========

where

* ⚪ = not yet implemented
* ❌ = not applicable
* ✔ = implemented
