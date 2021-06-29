.. _deploy-cephadm-nfs-ganesha:

===========
NFS Service
===========

.. note:: Only the NFSv4 protocol is supported.

The simplest way to manage NFS is via the ``ceph nfs cluster ...``
commands; see :ref:`mgr-nfs`.  This document covers how to manage the
cephadm services directly, which should only be necessary for unusual NFS
configurations.

Deploying NFS ganesha
=====================

Cephadm deploys NFS Ganesha daemon (or set of daemons).  The configuration for
NFS is stored in the ``nfs-ganesha`` pool and exports are managed via the
``ceph nfs export ...`` commands and via the dashboard.

To deploy a NFS Ganesha gateway, run the following command:

.. prompt:: bash #

    ceph orch apply nfs *<svc_id>* [--port *<port>*] [--placement ...]

For example, to deploy NFS with a service id of *foo* on the default
port 2049 with the default placement of a single daemon:

.. prompt:: bash #

   ceph orch apply nfs foo

See :ref:`orchestrator-cli-placement-spec` for the details of the placement
specification.

Service Specification
=====================

Alternatively, an NFS service can be applied using a YAML specification. 

.. code-block:: yaml

    service_type: nfs
    service_id: mynfs
    placement:
      hosts:
        - host1
        - host2
    spec:
      port: 12345

In this example, we run the server on the non-default ``port`` of
12345 (instead of the default 2049) on ``host1`` and ``host2``.

The specification can then be applied by running the following command:

.. prompt:: bash #

   ceph orch apply -i nfs.yaml


High-availability NFS
=====================

Deploying an *ingress* service for an existing *nfs* service will provide:

* a stable, virtual IP that can be used to access the NFS server
* fail-over between hosts if there is a host failure
* load distribution across multiple NFS gateways (although this is rarely necessary)

Ingress for NFS can be deployed for an existing NFS service
(``nfs.mynfs`` in this example) with the following specification:

.. code-block:: yaml

    service_type: ingress
    service_id: nfs.mynfs
    placement:
      count: 2
    spec:
      backend_service: nfs.mynfs
      frontend_port: 2049
      monitor_port: 9000
      virtual_ip: 10.0.0.123/24

A few notes:

  * The *virtual_ip* must include a CIDR prefix length, as in the
    example above.  The virtual IP will normally be configured on the
    first identified network interface that has an existing IP in the
    same subnet.  You can also specify a *virtual_interface_networks*
    property to match against IPs in other networks; see
    :ref:`ingress-virtual-ip` for more information.
  * The *monitor_port* is used to access the haproxy load status
    page.  The user is ``admin`` by default, but can be modified by
    via an *admin* property in the spec.  If a password is not
    specified via a *password* property in the spec, the auto-generated password
    can be found with:

    .. prompt:: bash #

	ceph config-key get mgr/cephadm/ingress.*{svc_id}*/monitor_password

    For example:

    .. prompt:: bash #

	ceph config-key get mgr/cephadm/ingress.nfs.myfoo/monitor_password
	
  * The backend service (``nfs.mynfs`` in this example) should include
    a *port* property that is not 2049 to avoid conflicting with the
    ingress service, which could be placed on the same host(s).
