===========
RGW Service
===========

.. _cephadm-deploy-rgw:

Deploy RGWs
===========

Cephadm deploys radosgw as a collection of daemons that manage a
single-cluster deployment or a particular *realm* and *zone* in a
multisite deployment.  (For more information about realms and zones,
see :ref:`multisite`.)

Note that with cephadm, radosgw daemons are configured via the monitor
configuration database instead of via a `ceph.conf` or the command line.  If
that configuration isn't already in place (usually in the
``client.rgw.<something>`` section), then the radosgw
daemons will start up with default settings (e.g., binding to port
80).

To deploy a set of radosgw daemons, with an arbitrary service name
*name*, run the following command:

.. prompt:: bash #

  ceph orch apply rgw *<name>* [--realm=*<realm-name>*] [--zone=*<zone-name>*] --placement="*<num-daemons>* [*<host1>* ...]"

Trivial setup
-------------

For example, to deploy 2 RGW daemons (the default) for a single-cluster RGW deployment
under the arbitrary service id *foo*:

.. prompt:: bash #

   ceph orch apply rgw foo

Designated gateways
-------------------

A common scenario is to have a labeled set of hosts that will act
as gateways, with multiple instances of radosgw running on consecutive
ports 8000 and 8001:

.. prompt:: bash #

   ceph orch host label add gwhost1 rgw  # the 'rgw' label can be anything
   ceph orch host label add gwhost2 rgw
   ceph orch apply rgw foo '--placement=label:rgw count-per-host:2' --port=8000

Multisite zones
---------------

To deploy RGWs serving the multisite *myorg* realm and the *us-east-1* zone on
*myhost1* and *myhost2*:

.. prompt:: bash #

   ceph orch apply rgw east --realm=myorg --zone=us-east-1 --placement="2 myhost1 myhost2"

Note that in a multisite situation, cephadm only deploys the daemons.  It does not create
or update the realm or zone configurations.  To create a new realm and zone, you need to do
something like:

.. prompt:: bash #

  radosgw-admin realm create --rgw-realm=<realm-name> --default
  
.. prompt:: bash #

  radosgw-admin zonegroup create --rgw-zonegroup=<zonegroup-name>  --master --default

.. prompt:: bash #

  radosgw-admin zone create --rgw-zonegroup=<zonegroup-name> --rgw-zone=<zone-name> --master --default

.. prompt:: bash #

  radosgw-admin period update --rgw-realm=<realm-name> --commit

See :ref:`orchestrator-cli-placement-spec` for details of the placement
specification.  See :ref:`multisite` for more information of setting up multisite RGW.


.. _orchestrator-haproxy-service-spec:

High availability service for RGW
=================================

This service allows the user to create a high avalilability RGW service
providing a minimun set of configuration options.

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
