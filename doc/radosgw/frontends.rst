.. _rgw_frontends:

==============
HTTP Frontends
==============

.. contents::

The Ceph Object Gateway supports two embedded HTTP frontend libraries
that can be configured with ``rgw_frontends``. See `Config Reference`_
for details about the syntax.

Beast
=====

.. versionadded:: Mimic

The ``beast`` frontend uses the Boost.Beast library for HTTP parsing
and the Boost.Asio library for asynchronous network i/o.

Options
-------

``port`` and ``ssl_port``

:Description: Sets the ipv4 & ipv6 listening port number. Can be specified multiple
              times as in ``port=80 port=8000``.
:Type: Integer
:Default: ``80``


``endpoint`` and ``ssl_endpoint``

:Description: Sets the listening address in the form ``address[:port]``, where
              the address is an IPv4 address string in dotted decimal form, or
              an IPv6 address in hexadecimal notation surrounded by square
              brackets. Specifying a IPv6 endpoint would listen to v6 only. The
              optional port defaults to 80 for ``endpoint`` and 443 for
              ``ssl_endpoint``. Can be specified multiple times as in
              ``endpoint=[::1] endpoint=192.168.0.100:8000``.

:Type: Integer
:Default: None


``ssl_certificate``

:Description: Path to the SSL certificate file used for SSL-enabled endpoints.
              If path is prefixed with ``config://``, the certificate will be
              pulled from the ceph monitor ``config-key`` database.

:Type: String
:Default: None


``ssl_private_key``

:Description: Optional path to the private key file used for SSL-enabled
              endpoints. If one is not given, the ``ssl_certificate`` file
              is used as the private key.
              If path is prefixed with ``config://``, the certificate will be
              pulled from the ceph monitor ``config-key`` database.

:Type: String
:Default: None

``ssl_options``

:Description: Optional colon separated list of ssl context options:

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

``ssl_ciphers``

:Description: Optional list of one or more cipher strings separated by colons.
              The format of the string is described in openssl's ciphers(1)
              manual.

:Type: String
:Default: None

``tcp_nodelay``

:Description: If set the socket option will disable Nagle's algorithm on 
              the connection which means that packets will be sent as soon 
              as possible instead of waiting for a full buffer or timeout to occur.

              ``1`` Disable Nagel's algorithm for all sockets.

              ``0`` Keep the default: Nagel's algorithm enabled.

:Type: Integer (0 or 1)
:Default: 0

``max_connection_backlog``

:Description: Optional value to define the maximum size for the queue of
              connections waiting to be accepted. If not configured, the value
              from ``boost::asio::socket_base::max_connections`` will be used.

:Type: Integer
:Default: None

``request_timeout_ms``

:Description: The amount of time in milliseconds that Beast will wait
              for more incoming data or outgoing data before giving up.
              Setting this value to 0 will disable timeout.

:Type: Integer
:Default: ``65000``

``max_header_size``

:Description: The maximum number of header bytes available for a single request.

:Type: Integer
:Default: ``16384``
:Maximum: ``65536``


Civetweb
========

.. versionadded:: Firefly
.. deprecated:: Pacific

The ``civetweb`` frontend uses the Civetweb HTTP library, which is a
fork of Mongoose.


Options
-------

``port``

:Description: Sets the listening port number. For SSL-enabled ports, add an
              ``s`` suffix like ``443s``. To bind a specific IPv4 or IPv6
              address, use the form ``address:port``. Multiple endpoints
              can either be separated by ``+`` as in ``127.0.0.1:8000+443s``,
              or by providing multiple options as in ``port=8000 port=443s``.

:Type: String
:Default: ``7480``


``num_threads``

:Description: Sets the number of threads spawned by Civetweb to handle
              incoming HTTP connections. This effectively limits the number
              of concurrent connections that the frontend can service.

:Type: Integer
:Default: ``rgw_thread_pool_size``


``request_timeout_ms``

:Description: The amount of time in milliseconds that Civetweb will wait
              for more incoming data before giving up.

:Type: Integer
:Default: ``30000``


``ssl_certificate``

:Description: Path to the SSL certificate file used for SSL-enabled ports.

:Type: String
:Default: None

``access_log_file``

:Description: Path to a file for access logs. Either full path, or relative
			  to the current working directory. If absent (default), then
			  accesses are not logged.

:Type: String
:Default: ``EMPTY``


``error_log_file``

:Description: Path to a file for error logs. Either full path, or relative
			  to the current working directory. If absent (default), then
			  errors are not logged.

:Type: String
:Default: ``EMPTY``


The following is an example of the ``/etc/ceph/ceph.conf`` file with some of these options set: ::
 
 [client.rgw.gateway-node1]
 rgw_frontends = civetweb request_timeout_ms=30000 error_log_file=/var/log/radosgw/civetweb.error.log access_log_file=/var/log/radosgw/civetweb.access.log

A complete list of supported options can be found in the `Civetweb User Manual`_.


Generic Options
===============

Some frontend options are generic and supported by all frontends:

``prefix``

:Description: A prefix string that is inserted into the URI of all
              requests. For example, a swift-only frontend could supply
              a uri prefix of ``/swift``.

:Type: String
:Default: None


.. _Civetweb User Manual: https://civetweb.github.io/civetweb/UserManual.html
.. _Config Reference: ../config-ref
