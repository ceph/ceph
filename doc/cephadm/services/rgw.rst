===========
RGW Service
===========

.. _cephadm-deploy-rgw:

Deploy RGWs
===========

Cephadm deploys the Object Gateway (RGW) as a collection of daemons that manage a
single-cluster deployment or a particular *realm* and *zone* in a
multisite deployment.  (For more information about realms and zones,
see :ref:`multisite`.)

Note that with ``cephadm``, ``radosgw`` daemons are configured via the monitor
configuration database instead of via a `ceph.conf` or the command line.  If
that configuration isn't already in place (usually in the
``client.rgw.<something>`` section), then the ``radosgw``
daemons will start up with default settings (e.g., binding to port
80).

To deploy a set of ``radosgw`` daemons, with an arbitrary service name
*name*, run the following command:

.. prompt:: bash #

  ceph orch apply rgw *<name>* [--realm=*<realm-name>*] [--zone=*<zone-name>*] --placement="*<num-daemons>* [*<host1>* ...]"

Trivial setup
-------------

For example, to deploy two daemons (the default) for a single-cluster RGW deployment
with the arbitrary service id ``foo``:

.. prompt:: bash #

   ceph orch apply rgw foo

.. _cephadm-rgw-designated_gateways:

Designated gateways
-------------------

A common scenario is to have a labeled set of hosts that will act
as gateways, with multiple instances of radosgw running on consecutive
ports 8000 and 8001:

.. prompt:: bash #

   ceph orch host label add gwhost1 rgw  # the 'rgw' label can be anything
   ceph orch host label add gwhost2 rgw
   ceph orch apply rgw foo '--placement=label:rgw count-per-host:2' --port=8000

See also: :ref:`cephadm_co_location`.

.. _cephadm-rgw-networks:

Specifying Networks
-------------------

The RGW service can have the network they bind to configured with a YAML service specification.

example spec file:

.. code-block:: yaml

    service_type: rgw
    service_id: foo
    placement:
      label: rgw
      count_per_host: 2
    networks:
    - 192.169.142.0/24
    spec:
      rgw_frontend_port: 8080

Passing Frontend Extra Arguments
--------------------------------

The RGW service specification can be used to pass extra arguments to the frontend by using
the ``rgw_frontend_extra_args`` arguments list.

example spec file:

.. code-block:: yaml

    service_type: rgw
    service_id: foo
    placement:
      label: rgw
      count_per_host: 2
    spec:
      rgw_realm: myrealm
      rgw_zone: myzone
      rgw_frontend_type: "beast"
      rgw_frontend_port: 5000
      rgw_frontend_extra_args:
      - "tcp_nodelay=1"
      - "max_header_size=65536"

.. note:: ``cephadm`` combines the arguments from the ``spec`` section with those from
	  ``rgw_frontend_extra_args`` into a single space-separated arguments list
	  which is used to set the value of the ``rgw_frontends`` configuration parameter.

Multisite zones
---------------

To deploy RGWs serving the multisite ``myorg`` realm and the ``us-east-1`` zone on
``myhost1`` and ``myhost2``:

.. prompt:: bash #

   ceph orch apply rgw east --realm=myorg --zonegroup=us-east-zg-1 --zone=us-east-1 --placement="2 myhost1 myhost2"

Note that in a multisite situation, ``cephadm`` only deploys the daemons.  It does not create
or update the realm or zone configurations.  To create a new realm, zone, and zonegroup
use :ref:`mgr-rgw-module` or issue commands of the following form:

.. prompt:: bash #

  radosgw-admin realm create --rgw-realm=<realm-name>

.. prompt:: bash #

  radosgw-admin zonegroup create --rgw-zonegroup=<zonegroup-name>  --master

.. prompt:: bash #

  radosgw-admin zone create --rgw-zonegroup=<zonegroup-name> --rgw-zone=<zone-name> --master

.. prompt:: bash #

  radosgw-admin period update --rgw-realm=<realm-name> --commit

See :ref:`orchestrator-cli-placement-spec` for details of the placement
specification.  See :ref:`multisite` for more information of setting up multisite RGW.

See also :ref:`multisite`.

Setting up HTTPS
----------------

In order to enable HTTPS for RGW services, apply a spec file following this scheme:

.. code-block:: yaml

  service_type: rgw
  service_id: myrgw
  spec:
    rgw_frontend_ssl_certificate: | 
      -----BEGIN PRIVATE KEY-----
      V2VyIGRhcyBsaWVzdCBpc3QgZG9vZi4gTG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFt
      ZXQsIGNvbnNldGV0dXIgc2FkaXBzY2luZyBlbGl0ciwgc2VkIGRpYW0gbm9udW15
      IGVpcm1vZCB0ZW1wb3IgaW52aWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWdu
      YSBhbGlxdXlhbSBlcmF0LCBzZWQgZGlhbSB2b2x1cHR1YS4gQXQgdmVybyBlb3Mg
      ZXQgYWNjdXNhbSBldCBqdXN0byBkdW8=
      -----END PRIVATE KEY-----
      -----BEGIN CERTIFICATE-----
      V2VyIGRhcyBsaWVzdCBpc3QgZG9vZi4gTG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFt
      ZXQsIGNvbnNldGV0dXIgc2FkaXBzY2luZyBlbGl0ciwgc2VkIGRpYW0gbm9udW15
      IGVpcm1vZCB0ZW1wb3IgaW52aWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWdu
      YSBhbGlxdXlhbSBlcmF0LCBzZWQgZGlhbSB2b2x1cHR1YS4gQXQgdmVybyBlb3Mg
      ZXQgYWNjdXNhbSBldCBqdXN0byBkdW8=
      -----END CERTIFICATE-----
    ssl: true

Then apply this yaml document:

.. prompt:: bash #

  ceph orch apply -i myrgw.yaml

Note the value of ``rgw_frontend_ssl_certificate`` is a literal string as
indicated by a ``|`` character preserving newline characters.

Setting up HTTPS with Wildcard SANs
-----------------------------------

To enable HTTPS for RGW services, apply a spec file following this scheme:

.. code-block:: yaml

  service_type: rgw
  service_id: foo
  placement:
    label: rgw
    count_per_host: 1
  spec:
    ssl: true
    generate_cert: true  
    rgw_frontend_port: 8080
    wildcard_enabled: true  # Enables wildcard SANs in the certificate
    zonegroup_hostnames:
    - s3.cephlab.com

Then apply this yaml document:

.. prompt:: bash #

  ceph orch apply -i myrgw.yaml

The ``wildcard_enabled`` flag ensures that a wildcard SAN entry is included in the self-signed certificate,
allowing access to buckets in virtual host mode. By default, this flag is disabled.
example: wildcard SAN - (*.s3.cephlab.com)

Disabling multisite sync traffic
--------------------------------

There is an RGW config option called ``rgw_run_sync_thread`` that tells the
RGW daemon to not transmit multisite replication data. This is useful if you want
that RGW daemon to be dedicated to I/O rather than multisite sync operations.
The RGW spec file includes a setting ``disable_multisite_sync_traffic`` that when
set to "True" will tell cephadm to set ``rgw_run_sync_thread`` to false for all
RGW daemons deployed for that RGW service. For example

.. code-block:: yaml

    service_type: rgw
    service_id: foo
    placement:
      label: rgw
    spec:
      rgw_realm: myrealm
      rgw_zone: myzone
      rgw_zonegroup: myzg
      disable_multisite_sync_traffic: True

.. note:: This will only stop the RGW daemon(s) from sending replication data.
    The daemon can still receive replication data unless it has been removed
    from the zonegroup and zone replication endpoints.

Draining client connections on shutdown
---------------------------------------

When an RGW daemon is stopped by for any reason, including during the cephadm upgrade process,
RGW offers a setting to delay shutdown as the RGW daemon attempts to complete ongoing
client requests. This setting is off by default but activated manually by either passing
``--stop-timeout=<timeout-in-seconds>`` to the RGW process or by setting the
``rgw_exit_timeout_secs`` config option for the RGW daemon. This value may be configured in
the RGW service spec file by specifying the ``rgw_exit_timeout_secs`` parameter in the spec
file. For example

.. code-block:: yaml

    service_type: rgw
    service_id: foo
    placement:
      label: rgw
    spec:
      rgw_realm: myrealm
      rgw_zone: myzone
      rgw_zonegroup: myzg
      rgw_exit_timeout_secs: 120

would tell the RGW daemons cephadm deploys for the rgw.foo service to wait up to 120
seconds for current client requests to complete. Note that the RGW daemon will refuse
new client requests during this time.

.. note:: In cephadm deployments this setting defaults to on and 120 seconds. If you would
    like to disable this feature you must set ``rgw_exit_timeout_secs`` to 0 in the spec

.. note:: Modifications to this setting in the spec will not be picked up by the RGW daemons
    in the service until they are redeployed using either the ``ceph orch redeploy <service-name>``
    or ``ceph orch daemon redeploy <daemon-name>`` commands


Service specification
---------------------

.. py:currentmodule:: ceph.deployment.service_spec

.. autoclass:: RGWSpec
   :members:

.. _orchestrator-haproxy-service-spec:

High availability service for RGW
=================================

The *ingress* service allows you to create a high availability endpoint
for RGW with a minimum set of configuration options.  The orchestrator will
deploy and manage a combination of haproxy and keepalived to provide load
balancing on a floating virtual IP.

If the RGW service is configured with SSL enabled, then the ingress service
will use the `ssl` and `verify none` options in the backend configuration.
Trust verification is disabled because the backends are accessed by IP 
address instead of FQDN.

.. image:: ../../images/HAProxy_for_RGW.svg

There are N hosts where the ingress service is deployed.  Each host
has a haproxy daemon and a keepalived daemon.  A virtual IP is
automatically configured on only one of these hosts at a time.

Each keepalived daemon checks every few seconds whether the haproxy
daemon on the same host is responding.  Keepalived will also check
that the master keepalived daemon is running without problems.  If the
"master" keepalived daemon or the active haproxy is not responding,
one of the remaining keepalived daemons running in backup mode will be
elected as master, and the virtual IP will be moved to that node.

The active haproxy acts like a load balancer, distributing all RGW requests
between all the RGW daemons available.

Prerequisites
-------------

* An existing RGW service.

Deploying
---------

Use the command::

    ceph orch apply -i <ingress_spec_file>

Service specification
---------------------

Service specs are YAML blocks with the following properties:

.. code-block:: yaml

    service_type: ingress
    service_id: rgw.something    # adjust to match your existing RGW service
    placement:
      hosts:
        - host1
        - host2
        - host3
    spec:
      backend_service: rgw.something            # adjust to match your existing RGW service
      virtual_ip: <string>/<string>             # ex: 192.168.20.1/24
      frontend_port: <integer>                  # ex: 8080
      monitor_port: <integer>                   # ex: 1967, used by haproxy for load balancer status
      virtual_interface_networks: [ ... ]       # optional: list of CIDR networks
      use_keepalived_multicast: <bool>          # optional: Default is False.
      vrrp_interface_network: <string>/<string> # optional: ex: 192.168.20.0/24
      health_check_interval: <string>           # optional: Default is 2s.
      ssl_cert: |                               # optional: SSL certificate and key
        -----BEGIN CERTIFICATE-----
        ...
        -----END CERTIFICATE-----
        -----BEGIN PRIVATE KEY-----
        ...
        -----END PRIVATE KEY-----

.. code-block:: yaml

    service_type: ingress
    service_id: rgw.something    # adjust to match your existing RGW service
    placement:
      hosts:
        - host1
        - host2
        - host3
    spec:
      backend_service: rgw.something      # adjust to match your existing RGW service
      virtual_ips_list:
      - <string>/<string>                 # ex: 192.168.20.1/24
      - <string>/<string>                 # ex: 192.168.20.2/24
      - <string>/<string>                 # ex: 192.168.20.3/24
      frontend_port: <integer>            # ex: 8080
      monitor_port: <integer>             # ex: 1967, used by haproxy for load balancer status
      virtual_interface_networks: [ ... ] # optional: list of CIDR networks
      first_virtual_router_id: <integer>  # optional: default 50
      health_check_interval: <string>     # optional: Default is 2s.
      ssl_cert: |                         # optional: SSL certificate and key
        -----BEGIN CERTIFICATE-----
        ...
        -----END CERTIFICATE-----
        -----BEGIN PRIVATE KEY-----
        ...
        -----END PRIVATE KEY-----


where the properties of this service specification are:

* ``service_type``
    Mandatory and set to "ingress"
* ``service_id``
    The name of the service.  We suggest naming this after the service you are
    controlling ingress for (e.g., ``rgw.foo``).
* ``placement hosts``
    The hosts where it is desired to run the HA daemons. An haproxy and a
    keepalived container will be deployed on these hosts.  These hosts do not need
    to match the nodes where RGW is deployed.
* ``virtual_ip``
    The virtual IP (and network) in CIDR format where the ingress service will be available.
* ``virtual_ips_list``
    The virtual IP address in CIDR format where the ingress service will be available.
    Each virtual IP address will be primary on one node running the ingress service. The number
    of virtual IP addresses must be less than or equal to the number of ingress nodes.
* ``virtual_interface_networks``
    A list of networks to identify which ethernet interface to use for the virtual IP.
* ``frontend_port``
    The port used to access the ingress service.
* ``ssl_cert``:
    SSL certificate, if SSL is to be enabled. This must contain the both the certificate and
    private key blocks in .pem format.
* ``use_keepalived_multicast``
    Default is False. By default, cephadm will deploy keepalived config to use unicast IPs,
    using the IPs of the hosts. The IPs chosen will be the same IPs cephadm uses to connect
    to the machines. But if multicast is prefered, we can set ``use_keepalived_multicast``
    to ``True`` and Keepalived will use multicast IP (224.0.0.18) to communicate between instances,
    using the same interfaces as where the VIPs are.
* ``vrrp_interface_network``
    By default, cephadm will configure keepalived to use the same interface where the VIPs are
    for VRRP communication. If another interface is needed, it can be set via ``vrrp_interface_network``
    with a network to identify which ethernet interface to use.
* ``first_virtual_router_id``
    Default is 50. When deploying more than 1 ingress, this parameter can be used to ensure each
    keepalived will have different virtual_router_id. In the case of using ``virtual_ips_list``,
    each IP will create its own virtual router. So the first one will have ``first_virtual_router_id``,
    second one will have ``first_virtual_router_id`` + 1, etc. Valid values go from 1 to 255.
* ``health_check_interval``
    Default is 2 seconds. This parameter can be used to set the interval between health checks
    for the haproxy with the backend servers.

.. _ingress-virtual-ip:

Selecting network interfaces for the virtual IP
------------------------------------------------

You cannot simply provide the name of the network interface on which
to configure the virtual IP because interface names may vary
across hosts (and/or reboots).  Instead, ``cephadm`` will select
interfaces based on other existing IP addresses that are already
configured.

Normally, the virtual IP will be configured on the first network
interface that has an existing IP in the same subnet.  For example, if
the virtual IP is 192.168.0.80/24 and eth2 has the static IP
192.168.0.40/24, cephadm will use eth2.

In some cases, the virtual IP may not belong to the same subnet as an existing static
IP.  In such cases, you can provide a list of subnets to match against existing IPs,
and cephadm will put the virtual IP on the first network interface to match.  For example,
if the virtual IP is 192.168.0.80/24 and we want it on the same interface as the machine's
static IP in 10.10.0.0/16, you can use a spec like::

  service_type: ingress
  service_id: rgw.something
  spec:
    virtual_ip: 192.168.0.80/24
    virtual_interface_networks:
      - 10.10.0.0/16
    ...

A consequence of this strategy is that you cannot currently configure the virtual IP
on an interface that has no existing IP address.  In this situation, we suggest
configuring a "dummy" IP address is an unroutable network on the correct interface
and reference that dummy network in the networks list (see above).


Useful hints for ingress
------------------------

* It is advised to have at least three ``radosgw`` daemons for availability and load balancing.
* We recommend at least three hosts for the ``ingress`` service.

Further Reading
===============

* :ref:`object-gateway`
* :ref:`mgr-rgw-module`
