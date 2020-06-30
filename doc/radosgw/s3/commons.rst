=================
 Common Entities
=================

.. toctree::
   :maxdepth: -1

Bucket and Host Name
--------------------
There are two different modes of accessing the buckets. The first (preferred) method
identifies the bucket as the top-level directory in the URI. ::

	GET /mybucket HTTP/1.1
	Host: cname.domain.com

The second method identifies the bucket via a virtual bucket host name. For example::

	GET / HTTP/1.1
	Host: mybucket.cname.domain.com

To configure virtual hosted buckets, you can either set ``rgw_dns_name = cname.domain.com`` in ceph.conf, or add ``cname.domain.com`` to the list of ``hostnames`` in your zonegroup configuration. See `Ceph Object Gateway - Multisite Configuration`_ for more on zonegroups.

.. tip:: We prefer the first method, because the second method requires expensive domain certification and DNS wild cards.

Common Request Headers
----------------------

+--------------------+------------------------------------------+
| Request Header     | Description                              |
+====================+==========================================+
| ``CONTENT_LENGTH`` | Length of the request body.              |
+--------------------+------------------------------------------+
| ``DATE``           | Request time and date (in UTC).          |
+--------------------+------------------------------------------+
| ``HOST``           | The name of the host server.             |
+--------------------+------------------------------------------+
| ``AUTHORIZATION``  | Authorization token.                     |
+--------------------+------------------------------------------+

Common Response Status
----------------------

+---------------+-----------------------------------+
| HTTP Status   | Response Code                     |
+===============+===================================+
| ``100``       | Continue                          |
+---------------+-----------------------------------+
| ``200``       | Success                           |
+---------------+-----------------------------------+
| ``201``       | Created                           |
+---------------+-----------------------------------+
| ``202``       | Accepted                          |
+---------------+-----------------------------------+
| ``204``       | NoContent                         |
+---------------+-----------------------------------+
| ``206``       | Partial content                   |
+---------------+-----------------------------------+
| ``304``       | NotModified                       |
+---------------+-----------------------------------+
| ``400``       | InvalidArgument                   |
+---------------+-----------------------------------+
| ``400``       | InvalidDigest                     |
+---------------+-----------------------------------+
| ``400``       | BadDigest                         |
+---------------+-----------------------------------+
| ``400``       | InvalidBucketName                 |
+---------------+-----------------------------------+
| ``400``       | InvalidObjectName                 |
+---------------+-----------------------------------+
| ``400``       | UnresolvableGrantByEmailAddress   |
+---------------+-----------------------------------+
| ``400``       | InvalidPart                       |
+---------------+-----------------------------------+
| ``400``       | InvalidPartOrder                  |
+---------------+-----------------------------------+
| ``400``       | RequestTimeout                    |
+---------------+-----------------------------------+
| ``400``       | EntityTooLarge                    |
+---------------+-----------------------------------+
| ``403``       | AccessDenied                      |
+---------------+-----------------------------------+
| ``403``       | UserSuspended                     |
+---------------+-----------------------------------+
| ``403``       | RequestTimeTooSkewed              |
+---------------+-----------------------------------+
| ``404``       | NoSuchKey                         |
+---------------+-----------------------------------+
| ``404``       | NoSuchBucket                      |
+---------------+-----------------------------------+
| ``404``       | NoSuchUpload                      |
+---------------+-----------------------------------+
| ``405``       | MethodNotAllowed                  |
+---------------+-----------------------------------+
| ``408``       | RequestTimeout                    |
+---------------+-----------------------------------+
| ``409``       | BucketAlreadyExists               |
+---------------+-----------------------------------+
| ``409``       | BucketNotEmpty                    |
+---------------+-----------------------------------+
| ``411``       | MissingContentLength              |
+---------------+-----------------------------------+
| ``412``       | PreconditionFailed                |
+---------------+-----------------------------------+
| ``416``       | InvalidRange                      |
+---------------+-----------------------------------+
| ``422``       | UnprocessableEntity               |
+---------------+-----------------------------------+
| ``500``       | InternalError                     |
+---------------+-----------------------------------+

.. _`Ceph Object Gateway - Multisite Configuration`: ../../multisite
