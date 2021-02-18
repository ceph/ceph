
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


.. _orchestrator-host-labels:

Host labels
-----------

The orchestrator supports assigning labels to hosts. Labels
are free form and have no particular meaning by itself and each host
can have multiple labels. They can be used to specify placement
of daemons. See :ref:`orch-placement-by-labels`

To add a label, run::

  ceph orch host label add my_hostname my_label

To remove a label, run::

  ceph orch host label rm my_hostname my_label



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


.. _orchestrator-haproxy-service-spec:

High availability service for RGW
=================================

This service allows the user to create a high avalilability RGW service
providing a mimimun set of configuration options.

The orchestrator will deploy and configure automatically several HAProxy and
Keepalived containers to assure the continuity of the RGW service while the
Ceph cluster will have at least 1 RGW daemon running.

The next image explains graphically how this service works:

.. image:: ../images/HAProxy_for_RGW.svg

There are N hosts where the HA RGW service is deployed. This means that we have
an HAProxy and a keeplived daemon running in each of this hosts.
Keepalived is used to provide a "virtual IP" binded to the hosts. All RGW
clients use this  "virtual IP"  to connect with the RGW Service.

Each keeplived daemon is checking each few seconds what is the status of the
HAProxy daemon running in the same host. Also it is aware that the "master" keepalived
daemon will be running without problems.

If the "master" keepalived daemon or the Active HAproxy is not responding, one
of the keeplived daemons running in backup mode will be elected as master, and
the "virtual ip" will be moved to that node.

The active HAProxy also acts like a load balancer, distributing all RGW requests
between all the RGW daemons available.

**Prerequisites:**

* At least two RGW daemons running in the Ceph cluster
* Operating system prerequisites:
  In order for the Keepalived service to forward network packets properly to the
  real servers, each router node must have IP forwarding turned on in the kernel.
  So it will be needed to set this system option::

    net.ipv4.ip_forward = 1

  Load balancing in HAProxy and Keepalived at the same time also requires the
  ability to bind to an IP address that are nonlocal, meaning that it is not
  assigned to a device on the local system. This allows a running load balancer
  instance to bind to an IP that is not local for failover.
  So it will be needed to set this system option::

    net.ipv4.ip_nonlocal_bind = 1

  Be sure to set properly these two options in the file ``/etc/sysctl.conf`` in
  order to persist this values even if the hosts are restarted.
  These configuration changes must be applied in all the hosts where the HAProxy for
  RGW service is going to be deployed.


**Deploy of the high availability service for RGW**

Use the command::

    ceph orch apply -i <service_spec_file>

**Service specification file:**

It is a yaml format file with the following properties:

.. code-block:: yaml

    service_type: ha-rgw
    service_id: haproxy_for_rgw
    placement:
      hosts:
        - host1
        - host2
        - host3
    spec:
      virtual_ip_interface: <string> # ex: eth0
      virtual_ip_address: <string>/<string> # ex: 192.168.20.1/24
      frontend_port: <integer>  # ex: 8080
      ha_proxy_port: <integer> # ex: 1967
      ha_proxy_stats_enabled: <boolean> # ex: true
      ha_proxy_stats_user: <string> # ex: admin
      ha_proxy_stats_password: <string> # ex: true
      ha_proxy_enable_prometheus_exporter: <boolean> # ex: true
      ha_proxy_monitor_uri: <string> # ex: /haproxy_health
      keepalived_user: <string> # ex: admin
      keepalived_password: <string> # ex: admin
      ha_proxy_frontend_ssl_certificate: <optional string> ex:
        [
          "-----BEGIN CERTIFICATE-----",
          "MIIDZTCCAk2gAwIBAgIUClb9dnseOsgJWAfhPQvrZw2MP2kwDQYJKoZIhvcNAQEL",
          ....
          "-----END CERTIFICATE-----",
          "-----BEGIN PRIVATE KEY-----",
          ....
          "sCHaZTUevxb4h6dCEk1XdPr2O2GdjV0uQ++9bKahAy357ELT3zPE8yYqw7aUCyBO",
          "aW5DSCo8DgfNOgycVL/rqcrc",
          "-----END PRIVATE KEY-----"
        ]
      ha_proxy_frontend_ssl_port: <optional integer> # ex: 8090
      ha_proxy_ssl_dh_param: <optional integer> # ex: 1024
      ha_proxy_ssl_ciphers: <optional string> # ex: ECDH+AESGCM:!MD5
      ha_proxy_ssl_options: <optional string> # ex: no-sslv3
      haproxy_container_image: <optional string> # ex: haproxy:2.4-dev3-alpine
      keepalived_container_image: <optional string> # ex: arcts/keepalived:1.2.2

where the properties of this service specification are:

* ``service_type``
    Mandatory and set to "ha-rgw"
* ``service_id``
    The name of the service.
* ``placement hosts``
    The hosts where it is desired to run the HA daemons. An HAProxy and a
    Keepalived containers will be deployed in these hosts.
    The RGW daemons can run in other different hosts or not.
* ``virtual_ip_interface``
    The physical network interface where the virtual ip will be binded
* ``virtual_ip_address``
    The virtual IP ( and network ) where the HA RGW service will be available.
    All your RGW clients must point to this IP in order to use the HA RGW
    service .
* ``frontend_port``
    The port used to access the HA RGW service
* ``ha_proxy_port``
    The port used by HAProxy containers
* ``ha_proxy_stats_enabled``
    If it is desired to enable the statistics URL in HAProxy daemons
* ``ha_proxy_stats_user``
    User needed to access the HAProxy statistics URL
* ``ha_proxy_stats_password``
    The password needed to access the HAProxy statistics URL
* ``ha_proxy_enable_prometheus_exporter``
    If it is desired to enable the Promethes exporter in HAProxy. This will
    allow to consume RGW Service metrics from Grafana.
* ``ha_proxy_monitor_uri``:
    To set the API endpoint where the health of HAProxy daemon is provided
* ``keepalived_user``
    User needed to access keepalived daemons
* ``keepalived_password``:
    The password needed to access keepalived daemons
* ``ha_proxy_frontend_ssl_certificate``:
    SSl certificate. You must paste the content of your .pem file
* ``ha_proxy_frontend_ssl_port``:
    The https port used by HAProxy containers
* ``ha_proxy_ssl_dh_param``:
    Value used for the `tune.ssl.default-dh-param` setting in the HAProxy
    config file
* ``ha_proxy_ssl_ciphers``:
    Value used for the `ssl-default-bind-ciphers` setting in HAProxy config
    file.
* ``ha_proxy_ssl_options``:
    Value used for the `ssl-default-bind-options` setting in HAProxy config
    file.
* ``haproxy_container_image``:
    HAProxy image location used to pull the image
* ``keepalived_container_image``:
    Keepalived image location used to pull the image

**Useful hints for the RGW Service:**

* Good to have at least 3 RGW daemons
* Use at least 3 hosts for the HAProxy for RGW service
* In each host an HAProxy and a Keepalived daemon will be deployed. These
  daemons can be managed as systemd services


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
