.. _deploy-cephadm-nfs-ganesha:

===========
NFS Service
===========

.. note:: Only the NFSv4 protocol is supported.

The simplest way to manage NFS is via the ``ceph nfs cluster ...``
commands; see :ref:`mgr-nfs`.  This document covers how to manage the
cephadm services directly, which should only be necessary for unusual NFS
configurations.

Deploying NFS Ganesha
=====================

Cephadm deploys NFS Ganesha daemon (or set of daemons).  The configuration for
NFS is stored in the ``.nfs`` pool and exports are managed via the
``ceph nfs export ...`` commands and via the dashboard.

To deploy a NFS Ganesha gateway, run the following command:

.. prompt:: bash #

    ceph orch apply nfs <svc_id> [--port <port>] [--placement ...]

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
    networks:
    - 1.2.3.4/24
    ip_addrs:
      host1: 10.0.0.100
      host2: 10.0.0.101
    spec:
      port: 12345
      monitoring_port: 567
      monitoring_ip_addrs:
        host1: 10.0.0.123
        host2: 10.0.0.124
      monitoring_networks:
      - 192.168.124.0/24


In this example, we run the server on the non-default ``port`` of
12345 (instead of the default 2049) on ``host1`` and ``host2``.
You can bind the NFS data port to a specific IP address using either the
``ip_addrs`` or ``networks`` section. If ``ip_addrs`` is provided and
the specified IP is assigned to the host, that IP will be used. If the
IP is not present but ``networks`` is specified, an IP matching one of
the given networks will be selected. If neither condition is met, the
daemon will not start on that node.
The default NFS monitoring port can be customized using the ``monitoring_port``
parameter. Additionally, you can specify the ``monitoring_ip_addrs`` or
``monitoring_networks`` parameters to bind the monitoring port to a specific
IP address or network. If ``monitoring_ip_addrs`` is provided and the specified
IP address is assigned to the host, that IP address will be used. If the IP
address is not present and ``monitoring_networks`` is specified, an IP address
that matches one of the specified networks will be used. If neither condition
is met, the default binding will happen on all available network interfaces.

NFS over RDMA
-------------

NFS over RDMA is disabled by default. To enable it, set ``enable_rdma: true`` in
the NFS service spec. You can optionally set ``rdma_port`` to use a custom RDMA
port, if omitted, NFS Ganesha uses its default.

When RDMA is enabled:

* New exports in the cluster default to **Transports = TCP, RDMA**
* For colocation, each entry in ``colocation_ports`` must include
  ``rdma_port`` in addition to ``data_port`` and ``monitoring_port``.

Example with RDMA enabled:

.. code-block:: yaml

    service_type: nfs
    service_id: mynfs
    placement:
      count: 1
      hosts: [host1]
    spec:
      port: 2049
      monitoring_port: 9587
      enable_rdma: true
      rdma_port: 20049   # optional

.. note:: If you use a bind address (e.g. ``virtual_ip``, ``ip_addrs``, or
   ``networks``) with ``enable_rdma``, ensure the network interface for that
   address is RDMA-capable. On the host, run ``rdma link show`` and confirm the
   netdev for the interface with the bind IP is listed.

NFS Daemon Colocation
----------------------

By default, cephadm avoids placing multiple NFS daemons on the same host. However,
you can enable colocation to deploy multiple NFS daemons on the same host for
increased capacity or redundancy.

.. note::
   When a host becomes unavailable, cephadm will automatically redeploy the
   affected NFS daemons on the remaining available hosts to maintain the desired
   ``count``. This may result in multiple daemons running on the same host,
   even if colocation was not explicitly configured. The system ensures that
   the total number of running daemons matches the specified count across
   all available hosts.

Colocation with Custom Ports
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For more control over port assignments, you can specify custom ports for colocated daemons
using the ``colocation_ports`` parameter:

.. code-block:: yaml

    service_type: nfs
    service_id: mynfs
    placement:
      count: 4
      hosts:
        - host1
        - host2
    spec:
      port: 2049
      monitoring_port: 9587
      colocation_ports:
        - data_port: 3049
          monitoring_port: 9588
        - data_port: 3050
          monitoring_port: 9589
        - data_port: 3051
          monitoring_port: 9590

In this configuration, 4 daemons total are deployed (2 per host), distributed across
``host1`` and ``host2``:

* **host1, daemon 1**: ``port: 2049`` and ``monitoring_port: 9587``
* **host1, daemon 2**: ``data_port: 3049`` and ``monitoring_port: 9588``
* **host2, daemon 1**: ``port: 2049`` and ``monitoring_port: 9587``
* **host2, daemon 2**: ``data_port: 3049`` and ``monitoring_port: 9588``

.. note::
   * The ``colocation_ports`` list defines ports for **additional** daemons only
     (2nd, 3rd, 4th, etc.). The first daemon always uses the base ``port`` and
     ``monitoring_port`` from the spec.
   * The number of entries in ``colocation_ports`` should be ``count - 1``,
     to cover the node down scenario (or ``count_per_host - 1`` when using ``count_per_host``).
   * Each entry must specify ``data_port``, ``monitoring_port``, and ``qos_cluster_port``.
     When ``enable_rdma`` is true, each entry must also include ``rdma_port``.
   * If ``colocation_ports`` is not specified, ports will be automatically
     incremented for colocated daemons (e.g., 2049 → 2050 → 2051 for data ports,
     and 9587 → 9588 → 9589 for monitoring ports).

Per-Host Colocation
~~~~~~~~~~~~~~~~~~~

You can also use ``count_per_host`` to specify exactly how many daemons should
run on each host:

.. code-block:: yaml

    service_type: nfs
    service_id: mynfs
    placement:
      count_per_host: 3
      hosts:
        - host1
        - host2
        - host3
    spec:
      port: 2049
      monitoring_port: 9587
      colocation_ports:
        - data_port: 3049
          monitoring_port: 9588
        - data_port: 4049
          monitoring_port: 9589

This will deploy exactly 3 NFS daemons on each of the 3 hosts (9 daemons total),
with custom ports for the 2nd and 3rd daemons on each host.

TLS/SSL Example
---------------

Here's an example NFS service specification with TLS/SSL configuration:

.. code-block:: yaml

    service_type: nfs
    service_id: mynfs
    placement:
      hosts:
      - ceph-node-0
    spec:
      port: 12345
      ssl: true
      certificate_source: inline|reference|cephadm-signed
      ssl_cert: |
        -----BEGIN CERTIFICATE-----
        (PEM cert contents here)
        -----END CERTIFICATE-----
      ssl_key: |
        -----BEGIN PRIVATE KEY-----
        (PEM key contents here)
        -----END PRIVATE KEY-----
      ssl_ca_cert:
        -----BEGIN PRIVATE KEY-----
        (PEM key contents here)
        -----END PRIVATE KEY-----
      tls_ktls: true
      tls_debug: true
      tls_min_version: TLSv1.3
      tls_ciphers: AES-256

This example configures an NFS service with TLS encryption enabled using
inline certificates.

TLS/SSL Parameters
~~~~~~~~~~~~~~~~~~

The following parameters can be used to configure TLS/SSL encryption for the NFS service:

* ``ssl`` (boolean): Enable or disable SSL/TLS encryption. Default is ``false``.

* ``certificate_source`` (string): Specifies the source of the TLS certificates.
  Options include:

  - ``cephadm-signed``: Use certificates signed by cephadm's internal CA
  - ``inline``: Provide certificates directly in the specification using ``ssl_cert``, ``ssl_key``, and ``ssl_ca_cert`` fields
  - ``reference``: Users can register their own certificate and key with certmgr and
    set the ``certificate_source`` to ``reference`` in the spec.

* ``ssl_cert`` (string): The SSL certificate in PEM format. Required when using
  ``inline`` certificate source.

* ``ssl_key`` (string): The SSL private key in PEM format. Required when using
  ``inline`` certificate source.

* ``ssl_ca_cert`` (string): The SSL CA certificate in PEM format. Required when
  using ``inline`` certificate source.

* ``custom_sans`` (list): List of custom Subject Alternative Names (SANs) to
  include in the certificate.

* ``tls_ktls`` (boolean): Enable kernel TLS (kTLS) for improved performance when
  available. Default is ``false``.

* ``tls_debug`` (boolean): Enable TLS debugging output. Useful for troubleshooting
  TLS issues. Default is ``false``.

* ``tls_min_version`` (string): Specify the minimum TLS version to accept.
  Examples: TLSv1.3, TLSv1.2

* ``tls_ciphers`` (string): Specify allowed cipher suites for TLS connections.
  Example: :-CIPHER-ALL:+AES-256-GCM

.. note:: When ``ssl`` is enabled, a ``certificate_source`` must be specified.
   If using ``inline`` certificates, all three certificate fields (``ssl_cert``,
   ``ssl_key``, ``ssl_ca_cert``) must be provided.

The specification can then be applied by running the following command:

.. prompt:: bash #

   ceph orch apply -i nfs.yaml

.. _cephadm-ha-nfs:

High-availability NFS
=====================

Deploying an *ingress* service for an existing *nfs* service will provide:

* a stable, virtual IP that can be used to access the NFS server
* failover between hosts if there is a host failure
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
      haproxy_peer_communication_port: <integer> # optional: NFS ingress only; HAProxy peer TCP port (default 1024)

A few notes:

  * The *virtual_ip* must include a CIDR prefix length, as in the
    example above.  The virtual IP will normally be configured on the
    first identified network interface that has an existing IP in the
    same subnet.  You can also specify a *virtual_interface_networks*
    property to match against IPs in other networks; see
    :ref:`ingress-virtual-ip` for more information.
  * The *monitor_port* is used to access the haproxy load status
    page.  The user is ``admin`` by default, but can be modified
    via an *admin* property in the spec.  If a password is not
    specified via a *password* property in the spec, the auto-generated password
    can be found with:

    .. prompt:: bash #

	ceph config-key get mgr/cephadm/ingress.<svc_id>/monitor_password

    For example:

    .. prompt:: bash #

	ceph config-key get mgr/cephadm/ingress.nfs.myfoo/monitor_password

  * The optional ``haproxy_peer_communication_port`` is used when ``backend_service``
    refers to an NFS service. HAProxy uses this TCP port for peer communication
    (stick-table synchronization between HAProxy instances on different hosts). The
    default is *1024*. Cephadm reserves this port alongside other ingress ports when
    scheduling daemons. Set a different value if *1024* is already in use or blocked.
    For RGW backends, HAProxy does not use a peers section, so this field is not applicable.

  * The backend service (``nfs.mynfs`` in this example) should include
    a *port* property that is not 2049 to avoid conflicting with the
    ingress service, which could be placed on the same host(s).

NFS with virtual IP but no haproxy
----------------------------------

Cephadm also supports deploying nfs with keepalived but not haproxy. This
offers a virtual IP supported by keepalived that the nfs daemon can directly bind
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


Further Reading
===============

* CephFS: :ref:`cephfs-nfs`
* MGR: :ref:`mgr-nfs`
