.. _rgw_ops_log:

===========
 Ops Log
===========

The RGW **ops log** records one line per HTTP request when ops logging is
enabled (see :confval:`rgw_enable_ops_log`). See `Destinations`_ for where the
entries go.

JSON-formatted ops logs (file or RADOS) include standard request metadata and,
for failed requests, error fields described below.

Destinations
============

The gateway writes each entry to every destination that is set:

- A file, named by ``rgw_ops_log_file_path``. It defaults to
  ``/var/log/ceph/ops-log-$cluster-$name.log``, so turning on the ops log is
  enough to get it in a file. A cephadm deployment rotates the file; other
  deployments should rotate it with ``logrotate`` or a similar tool.
- A Unix domain socket, named by :confval:`rgw_ops_log_socket_path`. See
  `Reading the Ops Log from a Socket`_.
- RADOS, when :confval:`rgw_ops_log_rados` is ``true``. Nothing trims these
  entries, so use this only with a plan to remove them with
  ``radosgw-admin log rm``.

The file and the socket receive the entries as JSON. Each holds up to
:confval:`rgw_ops_log_data_backlog` (5 MiB by default) of entries that have
not yet been written or read, and drops entries beyond that.

Reading the Ops Log from a Socket
=================================

When :confval:`rgw_ops_log_socket_path` is set, the gateway listens on that
socket. Each reader that connects receives ``[`` and then the JSON entries,
separated by a comma and a newline, for as long as it stays connected. The
array is never closed, and a reader that reconnects receives a new ``[``, so
the stream is not a complete JSON document. For example, to append the raw
stream to a file:

.. prompt:: bash #

   socat -u UNIX-CONNECT:/run/ceph/opslog.sock - >> /var/log/rgw-opslog.raw

For one JSON object per line, use the file destination instead.

Error fields
============

``http_status``
  HTTP status code sent to the client (string), e.g. ``403``, ``500``.

``error_code``
  S3-style ``Code`` string from the error response body when the request
  failed, e.g. ``QuotaExceeded``, ``ConnectionReset``. Empty on success.

``error_ret`` (since encoded struct version 17)
  Internal negative error number for the failure: POSIX ``errno`` (e.g.
  ``-104`` for ``ECONNRESET`` on Linux) or a positive RGW ``ERR_*`` constant
  encoded as negative (e.g. ``-2300`` for ``ERR_BUSY_RESHARDING``). Zero on
  success. Use this field to distinguish cases that share the same
  ``error_code`` or HTTP status in dashboards.

The daemon log line ``op status=…`` uses the same internal value as
``error_ret`` (without the field name).

HTTP error mapping
==================

RGW maps internal errors to HTTP status and ``error_code`` for S3 requests.
Some failures use **RGW-specific** ``Code`` strings (not defined in Amazon S3)
so operators can tell them apart in ops logs:

+---------------------------+-------------+----------------------+
| Internal (via ``error_ret``) | HTTP status | ``error_code``       |
+===========================+=============+======================+
| ``ECONNRESET``            | 500         | ``ConnectionReset``  |
| ``EPIPE``                 | 500         | ``BrokenPipe``       |
| ``ECONNABORTED``          | 500         | ``ConnectionAborted``|
| ``ECANCELED``             | 408         | ``RequestAborted``   |
| ``EIO``                   | 500         | ``InternalError``    |
| ``ERR_BUSY_RESHARDING``   | 503         | ``BucketResharding`` |
+===========================+=============+======================+

Other errno or ``ERR_*`` values continue to use the existing S3 mapping table;
unmapped errors appear as ``UnknownError`` (500) with a ``WARNING`` in the
``radosgw`` log.

``EBUSY`` (e.g. multipart meta lock contention) maps to ``ServiceUnavailable``
(503), not to the transport codes above.

Related settings
================

- :confval:`rgw_enable_ops_log`
- :confval:`rgw_log_http_headers`

See also :ref:`HTTP Request Errors <rgw_troubleshooting_http_errors>` in
:doc:`troubleshooting`.
