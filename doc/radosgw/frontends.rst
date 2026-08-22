.. _rgw_frontends:

==============
HTTP Frontends
==============

.. contents::

The Ceph Object Gateway supports two embedded HTTP frontend libraries
that can be configured with ``rgw_frontends``. See :ref:`radosgw-config-ref`
for details about the syntax.

Beast
=====

The ``beast`` frontend uses the Boost.Beast library for HTTP parsing
and the Boost.Asio library for asynchronous network I/O.

Options
-------

``port`` and ``ssl_port``

:Description: Sets the IPv4 & IPv6 listening port number. Can be specified multiple
              times as in ``port=80 port=8000``.
:Type: Integer
:Default: ``80``


``endpoint`` and ``ssl_endpoint``

:Description: Sets the listening address in the form ``address[:port]``, where
              the address is an IPv4 address string in dotted decimal form, or
              an IPv6 address in hexadecimal notation surrounded by square
              brackets. Specifying an IPv6 endpoint would listen to IPv6 only. The
              optional port defaults to 80 for ``endpoint`` and 443 for
              ``ssl_endpoint``. Can be specified multiple times as in
              ``endpoint=[::1] endpoint=192.168.0.100:8000``.

:Type: Integer
:Default: None


``ssl_certificate``

:Description: Path to the SSL certificate file used for SSL-enabled endpoints.
              If path is prefixed with ``config://``, the certificate will be
              pulled from the Ceph Monitor ``config-key`` database.

:Type: String
:Default: None


``ssl_private_key``

:Description: Optional path to the private key file used for SSL-enabled
              endpoints. If one is not given, the ``ssl_certificate`` file
              is used as the private key.
              If path is prefixed with ``config://``, the certificate will be
              pulled from the Ceph Monitor ``config-key`` database.

:Type: String
:Default: None

``ssl_reload``

:Description: Optional interval in seconds to periodically recreate the SSL
              context, which reloads the SSL certificate and private key from
              their specified paths. A value of ``0`` disables this feature.
              The reload is non-disruptive to existing connections. If the
              reload fails, the previous context continues to be used.

:Type: Integer
:Default: ``0``

``ssl_options``

:Description: Optional colon separated list of SSL context options:

              ``default_workarounds`` Implement various bug workarounds.

              ``no_compression`` Disable compression.

              ``no_sslv2`` Disable SSL v2.

              ``no_sslv3`` Disable SSL v3.

              ``no_tlsv1`` Disable TLS v1.

              ``no_tlsv1_1`` Disable TLS v1.1.

              ``no_tlsv1_2`` Disable TLS v1.2.

              ``single_dh_use`` Always create a new key when using tmp_dh parameters.

:Type: String
:Default: ``no_sslv2:no_sslv3:no_tlsv1:no_tlsv1_1``

``ssl_ciphers`` and ``ssl_ciphersuites``

:Description: Optional list of one or more cipher strings separated by colons.
              The format of the string is described in OpenSSL's ciphers(1)
              manual. The ``ssl_ciphers`` option only applies to connections
              using TLS v1.2 and below, while ``ssl_ciphersuites`` only applies
              to TLS v1.3.

:Type: String
:Default: None

``tls_groups``

:Description: Optional list of one or more `TLS Group`_ strings separated by colons.
              The pseudo group name ``DEFAULT`` can be used to select the OpenSSL
              built-in default list of groups. Other valid group names will depend on
              OpenSSL version. As of OpenSSL 3.5, names can be listed with commands
              ``openssl list -tls-groups`` and ``openssl list -all-tls-groups``.

:Type: String
:Default: None

``tcp_nodelay``

:Description: If set the socket option will disable Nagle's algorithm on 
              the connection which means that packets will be sent as soon 
              as possible instead of waiting for a full buffer or timeout to occur.

              ``1`` Disable Nagle's algorithm for all sockets.

              ``0`` Keep the default: Nagle's algorithm enabled.

:Type: Integer (0 or 1)
:Default: ``0``

``max_connection_backlog``

:Description: Optional value to define the maximum size for the queue of
              connections waiting to be accepted. If not configured, the value
              from ``boost::asio::socket_base::max_connections`` will be used.

:Type: Integer
:Default: None

``request_timeout_ms``

:Description: The amount of time in milliseconds that ``beast`` will wait
              for more incoming data or outgoing data before giving up.
              Setting this value to ``0`` will disable timeout.

:Type: Integer
:Default: ``65000``

``max_header_size``

:Description: The maximum number of header bytes available for a single request.

:Type: Integer
:Default: ``16384``
:Maximum: ``65536``

``so_reuseport``

:Description:  If set allows multiple RGW instances on a host to listen on the same TCP port.

              ``1`` Enable running multiple RGW on same port.

              ``0`` Disallow running multiple RGW on same port.

:Type: Integer (0 or 1)
:Default: ``0``




RGW-SMB Frontend
================

The ``rgw-smb`` frontend provides direct SMB protocol access to RGW buckets,
similar to how ``rgw-nfs`` provides NFS access. This runs as a library instance
serving the SMB protocol, distinct from the standard HTTP/S3 daemon instances.

.. note::
   The ``rgw-smb`` frontend is different from the SMB manager module. The SMB
   manager module provides SMB access to CephFS volumes via Samba containers,
   while the ``rgw-smb`` frontend provides direct SMB protocol access to RGW
   object storage. See :doc:`/mgr/smb` for information about the SMB manager module.

Configuration
-------------

The ``rgw-smb`` frontend is configured as a library instance serving the SMB protocol,
rather than using the :confval:`rgw_frontends` configuration option used by HTTP/S3 daemon instances.

.. important::
   **HTTP and SMB/NFS Protocols Can Coexist**

   When RGW is started as a library instance (``rgw-smb`` or ``rgw-nfs``),
   it will **not** start an HTTP listener by default. However, HTTP/S3 and SMB/NFS
   protocols can run together in the same instance. You can enable HTTP/S3
   access alongside SMB/NFS by configuring additional HTTP frontends using the
   protocol-specific :confval:`rgw_smb_frontends` or :confval:`rgw_nfs_frontends`
   configuration option.

Instance and Protocol Configuration:

- **Instance type**: Library (runs as ``librgw`` for SMB protocol)
- **Protocol type**: SMB
- **Config prefix**: ``rgw_smb_`` (underscore format for configuration keys)
- **Service name**: ``rgw-smb`` (hyphen format for service registration)

Options
-------

``rgw_smb_frontends``

:Description: Additional frontends to enable alongside the SMB protocol.
              Syntax is identical to :confval:`rgw_frontends`. This allows running
              both SMB and HTTP/S3 protocols from the same RGW instance.

:Type: String
:Default: rgw-smb

Running Multiple Frontends
---------------------------

The ``rgw-smb`` instance can serve multiple protocols simultaneously. This is
useful when you need to provide both SMB access and HTTP-based S3/Swift API
access from the same RGW instance.

**Example Configuration:**

.. code-block:: ini

   [client.rgw.smb-gateway]
   # Enable SMB frontend (default for rgw-smb daemon type)
   # Also enable HTTP frontend for S3/Swift API access
   rgw_smb_frontends = beast endpoint=0.0.0.0:8080 ssl_endpoint=0.0.0.0:8443 ssl_certificate=/path/to/cert.pem

In this configuration:

- The SMB protocol is enabled by default (instance type is library serving SMB)
- An HTTP/S3 frontend (Beast) is added to provide S3/Swift API access on port 8080 (HTTP) and 8443 (HTTPS)
- Both protocols share the same RGW backend and can access the same buckets and objects

.. note::
   When running multiple frontends, ensure that:

   - Port numbers do not conflict
   - Network security policies allow access to all configured ports
   - SSL certificates are properly configured for HTTPS endpoints
   - Authentication mechanisms are appropriate for each protocol

See :doc:`/mgr/smb` for detailed configuration and deployment information.


Generic Options
===============

Some frontend options are generic and supported by all frontends:

``prefix``

:Description: A prefix string that is inserted into the URI of all
              requests. For example, a swift-only frontend could supply
              a URI prefix of ``/swift``.

:Type: String
:Default: None


.. _TLS Group: https://openssl-library.org/post/2022-10-21-tls-groups-configuration/
