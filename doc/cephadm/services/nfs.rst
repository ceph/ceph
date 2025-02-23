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
NFS is stored in the ``.nfs`` pool and exports are managed via the
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

.. _cephadm-ha-nfs:

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

NFS with virtual IP but no haproxy
----------------------------------

Cephadm also supports deploying nfs with keepalived but not haproxy. This
offers a virtual ip supported by keepalived that the nfs daemon can directly bind
to instead of having traffic go through haproxy.

In this setup, you'll either want to set up the service using the nfs module
(see :ref:`nfs-module-cluster-create`) or place the ingress service first, so
the virtual IP is present for the nfs daemon to bind to. The ingress service
should include the attribute ``keepalive_only`` set to true. For example

.. code-block:: yaml

    service_type: ingress
    service_id: nfs.foo
    placement:
      count: 1
      hosts:
      - host1
      - host2
      - host3
    spec:
      backend_service: nfs.foo
      monitor_port: 9049
      virtual_ip: 192.168.122.100/24
      keepalive_only: true

Then, an nfs service could be created that specifies a ``virtual_ip`` attribute
that will tell it to bind to that specific IP.

.. code-block:: yaml

    service_type: nfs
    service_id: foo
    placement:
      count: 1
      hosts:
      - host1
      - host2
      - host3
    spec:
      port: 2049
      virtual_ip: 192.168.122.100

Note that in these setups, one should make sure to include ``count: 1`` in the
nfs placement, as it's only possible for one nfs daemon to bind to the virtual IP.

NFS with HAProxy Protocol Support
----------------------------------

Cephadm supports deploying NFS in High-Availability mode with additional
HAProxy protocol support. This works just like High-availability NFS but also
supports client IP level configuration on NFS Exports.  This feature requires
`NFS-Ganesha v5.0`_ or later.

.. _NFS-Ganesha v5.0: https://github.com/nfs-ganesha/nfs-ganesha/wiki/ReleaseNotes_5

To use this mode, you'll either want to set up the service using the nfs module
(see :ref:`nfs-module-cluster-create`) or manually create services with the
extra parameter ``enable_haproxy_protocol`` set to true. Both NFS Service and
Ingress service must have ``enable_haproxy_protocol`` set to the same value.
For example:

.. code-block:: yaml

    service_type: ingress
    service_id: nfs.foo
    placement:
      count: 1
      hosts:
      - host1
      - host2
      - host3
    spec:
      backend_service: nfs.foo
      monitor_port: 9049
      virtual_ip: 192.168.122.100/24
      enable_haproxy_protocol: true

.. code-block:: yaml

    service_type: nfs
    service_id: foo
    placement:
      count: 1
      hosts:
      - host1
      - host2
      - host3
    spec:
      port: 2049
      enable_haproxy_protocol: true


NFS Encryption with Stunnel TLS
----------------------------------

Cephadm supports deploying NFS with a sidecar Stunnel container
to provide another endpoint with TLS encryption. Clients will need
to run a Stunnel client and mount from the client. The Stunnel image
is based on `alpine-stunnel`_ with some modifications to the run script.

There are self-signed default certificates present in the image,
however, it is strongly recommended to use your own certificates.

.. _alpine-stunnel: https://github.com/flitbit/alpine-stunnel

To use this feature, you'll want to create certificates on every host that the NFS
server is going to be placed on. This is done by creating a certificate directory
and having files named `cert.pem`, `key.pem` and `ca.pem` available.
The `stunnel_tls_dir` option allows for specifying the path to this folder on
the NFS hosts which is mounted into the Stunnel container under `/etc/tls`.

A port for Stunnel can be specified otherwise it defaults to `20490`.

.. code-block:: yaml

    service_type: nfs
    service_id: foo
    placement:
      count: 1
      hosts:
      - host1
      - host2
      - host3
    spec:
      port: 2049
      deploy_stunnel: true
      stunnel_tls_dir: /etc/mynfscerts
      stunnel_port: 20490

The same container image that is used as a server can be used as the client.
In the example below, we run a Stunnel client container and can use the
client's own IP and port used in the `docker/podman run` command to mount the NFS share.

::
    docker run -v /etc/mynfscerts:/etc/tls --network host quay.io/adk3798/stunnel:0.1.0.build-5 --endpoint {nfs_ip/virtual_ip}:{stunnel_port} --bind 0.0.0.0 --port 2049

    mount -t nfs 127.0.0.1:2049/test /mnt/nfs

Some more information about NFS encryption using Stunnel can
be found in `Encrypting NFSv4 with Stunnel TLS`_ article.

.. _Encrypting NFSv4 with Stunnel TLS: https://www.linuxjournal.com/content/encrypting-nfsv4-stunnel-tls



Further Reading
===============

* CephFS: :ref:`cephfs-nfs`
* MGR: :ref:`mgr-nfs`
