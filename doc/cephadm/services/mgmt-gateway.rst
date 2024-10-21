.. _deploy-cephadm-mgmt-gateway:

==================
Management Gateway
==================

Deploying mgmt-gateway
======================

In Ceph releases beginning with Squid, the `mgmt-gateway` service introduces a new design for Ceph applications
based on a modular, service-based architecture. This service, managed by cephadm and built on top of nginx
(an open-source, high-performance web server), acts as the new front-end and single entry point to the
Ceph cluster. The `mgmt-gateway` provides unified access to all Ceph applications, including the Ceph dashboard
and monitoring stack. Employing nginx enhances security and simplifies access management due to its robust
community support and high-security standards. The `mgmt-gateway` service acts as a reverse proxy that routes
requests to the appropriate Ceph application instances.

In order to deploy the mgmt-gateway service, use the following command:

.. prompt:: bash #

    ceph orch apply mgmt-gateway [--placement ...] ...

Once applied cephadm will reconfigure specific running daemons (such as monitoring) to run behind the
new created service. External access to those services will not be possible anymore. Access will be
consolidated behind the new service endpoint: `https://<node-ip>:<port>`.


Benefits of the mgmt-gateway service
====================================
* ``Unified Access``: Consolidated access through nginx improves security and provide a single entry point to services.
* ``Improved user experience``: User no longer need to know where each application is running (ip/host).
* ``High Availability for dashboard``: nginx HA mechanisms are used to provide high availability for the Ceph dashboard.
* ``High Availability for monitoring``: nginx HA mechanisms are used to provide high availability for monitoring.

Security enhancements
=====================

Once the `mgmt-gateway` service is deployed user cannot access monitoring services without authentication through the
Ceph dashboard.


High availability enhancements
==============================
nginx HA mechanisms are used to provide high availability for all the Ceph management applications including the Ceph dashboard
and monitoring stack. In case of the Ceph dashboard user no longer need to know where the active manager is running.
`mgmt-gateway` handles manager failover transparently and redirects the user to the active manager. In case of the
monitoring `mgmt-gateway` takes care of handling HA when several instances of Prometheus, Alertmanager or Grafana are
available. The reverse proxy will automatically detect healthy instances and use them to process user requests.


High Availability for mgmt-gateway service
==========================================

In addition to providing high availability for the underlying backend services, the mgmt-gateway
service itself can be configured for high availability, ensuring that the system remains resilient
even if certain core components for the service fail.

Multiple mgmt-gateway instances can be deployed in an active/standby configuration using keepalived
for seamless failover. The `oauth2-proxy` service can be deployed as multiple stateless instances,
with nginx acting as a load balancer across them using round-robin strategy. This setup removes
single points of failure and enhances the resilience of the entire system.

In this setup, the underlying internal services follow the same high availability mechanism. Instead of
directly accessing the `mgmt-gateway` internal endpoint, services use the virtual IP specified in the spec.
This ensures that the high availability mechanism for `mgmt-gateway` is transparent to other services.

Example Configuration for High Availability

To deploy the mgmt-gateway in a high availability setup, here is an example of the specification files required:

`mgmt-gateway` Configuration:

.. code-block:: yaml

    service_type: mgmt-gateway
    placement:
      label: mgmt
    spec:
      enable_auth: true
      virtual_ip: 192.168.100.220

`Ingress` Configuration for Keepalived:

.. code-block:: yaml

    service_type: ingress
    service_id: ingress-mgmt-gw
    placement:
      label: mgmt
    virtual_ip: 192.168.100.220
    backend_service: mgmt-gateway
    keepalive_only: true

The number of deployed instances is determined by the number of hosts with the mgmt label.
The ingress is configured in `keepalive_only` mode, with labels ensuring that any changes to
the mgmt-gateway daemons are replicated to the corresponding keepalived instances. Additionally,
the `virtual_ip` parameter must be identical in both specifications.


Accessing services with mgmt-gateway
====================================

Once the `mgmt-gateway` service is deployed direct access to the monitoring services will not be allowed anymore.
Applications including: Prometheus, Grafana and Alertmanager are now accessible through links
from `Administration > Services`.


Service Specification
=====================

A mgmt-gateway service can be applied using a specification. An example in YAML follows:

.. code-block:: yaml

    service_type: mgmt-gateway
    service_id: gateway
    placement:
      hosts:
        - ceph0
    spec:
     port: 5000
     ssl_protocols:
       - TLSv1.2
       - TLSv1.3
       - ...
     ssl_ciphers:
       - AES128-SHA
       - AES256-SHA
       - ...
     ssl_certificate: |
       -----BEGIN CERTIFICATE-----
       MIIDtTCCAp2gAwIBAgIYMC4xNzc1NDQxNjEzMzc2MjMyXzxvQ7EcMA0GCSqGSIb3
       DQEBCwUAMG0xCzAJBgNVBAYTAlVTMQ0wCwYDVQQIDARVdGFoMRcwFQYDVQQHDA5T
       [...]
       -----END CERTIFICATE-----
    ssl_certificate_key: |
       -----BEGIN PRIVATE KEY-----
       MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC5jdYbjtNTAKW4
       /CwQr/7wOiLGzVxChn3mmCIF3DwbL/qvTFTX2d8bDf6LjGwLYloXHscRfxszX/4h
       [...]
       -----END PRIVATE KEY-----

Fields specific to the ``spec`` section of the mgmt-gateway service are described below.

.. py:currentmodule:: ceph.deployment.service_spec

.. autoclass:: MgmtGatewaySpec
   :members:

.. warning::

   TLSv1.3 is considered safe at this moment and includes a set of secure ciphers by default.
   When configuring SSL/TLS ciphers for older versions, especially TLSv1.2, it is crucial to
   use only a subset of secure ciphers. Using weak or outdated ciphers can significantly
   compromise the security of your system.

   Any alteration of the cipher list for SSL/TLS configurations is the responsibility of the
   system administrator. Avoid modifying these lists without a thorough understanding of the
   implications. Incorrect configurations can lead to vulnerabilities such as weak encryption,
   lack of forward secrecy, and susceptibility to various attacks. Always refer to up-to-date
   security guidelines and best practices when configuring SSL/TLS settings.


The specification can then be applied by running the following command:

.. prompt:: bash #

   ceph orch apply -i mgmt-gateway.yaml


Limitations
===========

* Services must bind to the appropriate ports based on the applications being proxied. Ensure that there
  are no port conflicts that might disrupt service availability.


Default images
~~~~~~~~~~~~~~

The `mgmt-gateway` service internally makes use of nginx reverse proxy. The following container image is used by default:

::

    mgr/cephadm/container_image_nginx = 'quay.io/ceph/nginx:sclorg-nginx-126'

Admins can specify the image to be used by changing the `container_image_nginx` cephadm module option. If there were already
running daemon(s) you must redeploy the daemon(s) in order to have them actually use the new image.

For example:

.. code-block:: bash

     ceph config set mgr mgr/cephadm/container_image_nginx <new-nginx-image>
     ceph orch redeploy mgmt-gateway
