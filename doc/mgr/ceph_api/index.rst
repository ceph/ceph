.. _mgr ceph api:

================
Ceph RESTful API
================

Introduction
============
The **Ceph RESTful API** (henceforth **Ceph API**) is provided by the
:ref:`mgr-dashboard` module. The Ceph API
service is available at the same URL as the regular Ceph Dashboard, under the
``/api`` base path (please refer to :ref:`dashboard-host-name-and-port`)::

  http://<server_addr>:<server_port>/api

or, if HTTPS is enabled (please refer to :ref:`dashboard-ssl-tls-support`)::

  https://<server_addr>:<ssl_server_port>/api

The Ceph API leverages the following standards:

* `HTTP 1.1 <https://tools.ietf.org/html/rfc7231>`_ for API syntax and semantics,
* `JSON <https://tools.ietf.org/html/rfc8259>`_ for content encoding,
* `HTTP Content Negotiation <https://tools.ietf.org/html/rfc2295>`_ and `MIME <https://tools.ietf.org/html/rfc2045>`_ for versioning,
* `OAuth 2.0 <https://tools.ietf.org/html/rfc6750>`_ and `JWT <https://tools.ietf.org/html/rfc7519>`_ for authentication and authorization.

.. warning::
  Some endpoints are still under active development, and should be carefully
  used since new Ceph releases could bring backward incompatible changes.


Authentication and Authorization
================================

Requests to the Ceph API pass through two access control checkpoints:

* **Authentication**: ensures that the request is performed on behalf of an existing and valid user account.
* **Authorization**: ensures that the previously authenticated user can in fact perform a specific action (create, read, update or delete) on the target endpoint.

So, prior to start consuming the Ceph API, a valid JSON Web Token (JWT) has to
be obtained, and it may then be reused for subsequent requests. The
``/api/auth`` endpoint will provide the valid token:

.. prompt:: bash $

   curl -X POST "https://example.com:8443/api/auth" \
   -H  "Accept: application/vnd.ceph.api.v1.0+json" \
   -H  "Content-Type: application/json" \
   -d '{"username": <username>, "password": <password>}'

::

    { "token": "<redacted_token>", ...}

The token obtained must be passed together with every API request in the
``Authorization`` HTTP header:

.. prompt:: bash $

   curl -H "Authorization: Bearer <token>" ...

Authentication and authorization can be further configured from the
Ceph CLI, the Ceph-Dashboard UI and the Ceph API itself (please refer to
:ref:`dashboard-user-role-management`).

Versioning
==========

One of the main goals of the Ceph API is to keep a stable interface. For this
purpose, Ceph API is built upon the following principles:

* **Mandatory**: in order to avoid implicit defaults, all endpoints require an explicit default version (starting with ``1.0``).
* **Per-endpoint**: as this API wraps many different Ceph components, this allows for a finer-grained change control.
   * **Content/MIME Type**: the version expected from a specific endpoint is stated by the ``Accept: application/vnd.ceph.api.v<major>.<minor>+json`` HTTP header. If the current Ceph API server is not able to address that specific major version, a `415 - Unsupported Media Type <https://tools.ietf.org/html/rfc7231#section-6.5.13>`_ response will be returned.
* **Semantic Versioning**: with a ``major.minor`` version:
   * Major changes are backward incompatible: they might result in non-additive changes to the request and/or response formats of a specific endpoint.
   * Minor changes are backward/forward compatible: they basically consists of additive changes to the request or response formats of a specific endpoint.

An example:

.. prompt:: bash $

   curl -X GET "https://example.com:8443/api/osd" \
   -H  "Accept: application/vnd.ceph.api.v1.0+json" \
   -H  "Authorization: Bearer <token>"


.. _mgr ceph api docs:

Accessing the API Documentation
===============================

Every running Ceph Dashboard serves documentation for the Ceph API that matches
the version of Ceph that is running. This documentation is available in the
following forms:

* **Interactive documentation**: Click the help icon (**?**) in the top
  navigation bar of the Dashboard and select **API**. This opens an
  interactive `Swagger UI <https://swagger.io/tools/swagger-ui/>`_ page that
  lists every endpoint together with its parameters, request bodies and
  responses. The page can also be opened directly at::

    https://<server_addr>:<ssl_server_port>/#/api-docs

  To send requests from this page by using **Try it out**, first obtain a
  token from the ``/api/auth`` endpoint (see `Authentication and
  Authorization`_), then click **Authorize** and enter the token.

* **OpenAPI specification**: The raw `OpenAPI 3.0
  <https://spec.openapis.org/oas/v3.0.3>`_ specification, in JSON format, is
  available at::

    https://<server_addr>:<ssl_server_port>/docs/openapi.json

  This document does not require authentication and can be loaded into any
  OpenAPI-compatible tool, for example to generate API clients:

  .. prompt:: bash $

     curl -o openapi.json "https://example.com:8443/docs/openapi.json"

If HTTPS is disabled, use ``http://<server_addr>:<server_port>`` as the base
URL instead (see :ref:`dashboard-host-name-and-port`).

The specification of the Ceph API for the current release is also included in
the `Specification`_ section below.


Specification
=============

.. openapi:: ../../../src/pybind/mgr/dashboard/openapi.yaml
    :group:
    :examples:
    :encoding: utf-8
