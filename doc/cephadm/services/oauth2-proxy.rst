.. _deploy-cephadm-oauth2-proxy:

==================
OAuth2 Proxy
==================

Deploying oauth2-proxy
======================

In Ceph releases starting from Squid, the `oauth2-proxy` service introduces an advanced method
for managing authentication and access control for Ceph applications. This service integrates
with external Identity Providers (IDPs) to provide secure, flexible authentication via the
OIDC (OpenID Connect) protocol. `oauth2-proxy` acts as an authentication gateway, ensuring that
access to Ceph applications including the Ceph Dashboard and monitoring stack is tightly controlled.

To deploy the `oauth2-proxy` service, use the following command:

.. prompt:: bash #

    ceph orch apply oauth2-proxy [--placement ...] ...

Once applied, `cephadm` will re-configure the necessary components to use `oauth2-proxy` for authentication,
thereby securing access to all Ceph applications. The service will handle login flows, redirect users
to the appropriate IDP for authentication, and manage session tokens to facilitate seamless user access.


Benefits of the oauth2-proxy service
====================================
* ``Enhanced Security``: Provides robust authentication through integration with external IDPs using the OIDC protocol.
* ``Seamless SSO``: Enables seamless single sign-on (SSO) across all Ceph applications, improving user access control.
* ``Centralized Authentication``: Centralizes authentication management, reducing complexity and improving control over access.


Security enhancements
=====================

The `oauth2-proxy` service ensures that all access to Ceph applications is authenticated, preventing unauthorized users from
accessing sensitive information. Since it makes use of the `oauth2-proxy` open source project, this service integrates
easily with a variety of `external IDPs <https://oauth2-proxy.github.io/oauth2-proxy/configuration/providers/>`_ to provide
a secure and flexible authentication mechanism.


High availability
==============================
In general, `oauth2-proxy` is used in conjunction with the `mgmt-gateway`. The `oauth2-proxy` service can be deployed as multiple
stateless instances, with the `mgmt-gateway` (nginx reverse-proxy) handling load balancing across these instances using a round-robin strategy.
Since oauth2-proxy integrates with an external identity provider (IDP), ensuring high availability for login is managed externally
and not the responsibility of this service.


Accessing services with oauth2-proxy
====================================

After deploying `oauth2-proxy`, access to Ceph applications will require authentication through the configured IDP. Users will
be redirected to the IDP for login and then returned to the requested application. This setup ensures secure access and integrates
seamlessly with the Ceph management stack.


Service Specification
=====================

Before deploying `oauth2-proxy` service please remember to deploy the `mgmt-gateway` service by turning on the `--enable_auth` flag. i.e:

.. prompt:: bash #

   ceph orch apply mgmt-gateway --enable_auth=true

An `oauth2-proxy` service can be applied using a specification. An example in YAML follows:

.. code-block:: yaml

    service_type: oauth2-proxy
    service_id: auth-proxy
    placement:
      label: mgmt
    spec:
     https_address: "0.0.0.0:4180"
     provider_display_name: "My OIDC Provider"
     client_id: "your-client-id"
     oidc_issuer_url: "http://192.168.100.1:5556/dex"
     client_secret: "your-client-secret"
     cookie_secret: "your-cookie-secret"
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

Fields specific to the ``spec`` section of the `oauth2-proxy` service are described below. More detailed
description of the fields can be found on `oauth2-proxy <https://oauth2-proxy.github.io/oauth2-proxy/>`_
project documentation.


.. py:currentmodule:: ceph.deployment.service_spec

.. autoclass:: OAuth2ProxySpec
   :members:

The specification can then be applied by running the below command. Once becomes available, cephadm will automatically redeploy
the `mgmt-gateway` service while adapting its configuration to redirect the authentication to the newly deployed `oauth2-service`.

.. prompt:: bash #

   ceph orch apply -i oauth2-proxy.yaml


Limitations
===========

A non-exhaustive list of important limitations for the `oauth2-proxy` service follows:

* High-availability configurations for `oauth2-proxy` itself are not supported.
* Proper configuration of the IDP and OAuth2 parameters is crucial to avoid authentication failures. Misconfigurations can lead to access issues.


Container images
~~~~~~~~~~~~~~~~

The container image the `oauth2-proxy` service will use can be found by running:

::

    ceph config get mgr mgr/cephadm/container_image_oauth2_proxy

Admins can specify a custom image to be used by changing the `container_image_oauth2_proxy` cephadm module option.
If there were already running daemon(s), you must also redeploy the daemon(s) for them to use the new image.

For example:

.. code-block:: bash

     ceph config set mgr mgr/cephadm/container_image_oauth2_proxy <new-oauth2-proxy-image>
     ceph orch redeploy oauth2-proxy
