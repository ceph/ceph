=================
 Common Entities
=================

.. toctree::
   :maxdepth: -1

Bucket and Host Name
--------------------
There are two different modes of accessing buckets. The first method identifies
the bucket as the top-level directory in the URI::

	GET /mybucket HTTP/1.1
	Host: cname.domain.com

Most S3 clients nowadays rely on vhost-style access. The desired bucket is
indicated by a DNS FQDN. For example::

	GET / HTTP/1.1
	Host: mybucket.cname.domain.com

The second method is deprecated by AWS. See the `Amazon S3 Path Deprecation
Plan`_ for more information.

To configure virtual hosted buckets, you can either set ``rgw_dns_name =
cname.domain.com`` in ``ceph.conf`` or add ``cname.domain.com`` to the list of
``hostnames`` in your zonegroup configuration. See `Ceph Object Gateway -
Multisite Configuration`_ for more on zonegroups.

Here is an example of a ``ceph config set`` comamnd that sets ``rgw_dns_name``
to ``cname.domain.com``:

.. prompt:: bash $

   ceph config set client.rgw.<ceph authx client for rgw> rgw_dns_name cname.domain.dom

.. tip:: You can define multiple hostnames directly with the
   :confval:`rgw_dns_name` parameter.

.. tip:: When SSL is enabled, the certificates must use a wildcard in the
   domain name in order to match the bucket subdomains.

.. note:: When Ceph Object Gateways are behind a proxy, use the proxy's DNS
   name instead. Then you can use ``ceph config set client.rgw`` to set the DNS
   name for all instances.
   
.. note:: The static website view for the `s3website` API must be served under
   a different domain name. This is configured separately from
   :confval:`rgw_dns_name`, in :confval:`rgw_dns_s3website_name`.


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
.. _`Amazon S3 Path Deprecation Plan`: https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/
