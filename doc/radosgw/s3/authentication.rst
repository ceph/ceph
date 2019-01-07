=========================
 Authentication and ACLs
=========================

Requests to the RADOS Gateway (RGW) can be either authenticated or 
unauthenticated. RGW assumes unauthenticated requests are sent by an anonymous 
user. RGW supports canned ACLs.

Authentication
--------------
Authenticating a request requires including an access key and a Hash-based 
Message Authentication Code (HMAC) in the request before it is sent to the 
RGW server. RGW uses an S3-compatible authentication approach. 

::

	HTTP/1.1
	PUT /buckets/bucket/object.mpeg
	Host: cname.domain.com
	Date: Mon, 2 Jan 2012 00:01:01 +0000
	Content-Encoding: mpeg	
	Content-Length: 9999999

	Authorization: AWS {access-key}:{hash-of-header-and-secret}

In the foregoing example, replace ``{access-key}`` with the value for your access 
key ID followed by a colon (``:``). Replace ``{hash-of-header-and-secret}`` with 
a hash of the header string and the secret corresponding to the access key ID.

To generate the hash of the header string and secret, you must:

#. Get the value of the header string.
#. Normalize the request header string into canonical form. 
#. Generate an HMAC using a SHA-1 hashing algorithm.
   See `RFC 2104`_ and `HMAC`_ for details.
#. Encode the ``hmac`` result as base-64.

To normalize the header into canonical form: 

#. Get all fields beginning with ``x-amz-``.
#. Ensure that the fields are all lowercase.
#. Sort the fields lexicographically. 
#. Combine multiple instances of the same field name into a 
   single field and separate the field values with a comma.
#. Replace white space and line breaks in field values with a single space.
#. Remove white space before and after colons.
#. Append a new line after each field.
#. Merge the fields back into the header.

Replace the ``{hash-of-header-and-secret}`` with the base-64 encoded HMAC string.

Authentication against OpenStack Keystone
-----------------------------------------

In a radosgw instance that is configured with authentication against
OpenStack Keystone, it is possible to use Keystone as an authoritative
source for S3 API authentication. To do so, you must set:

* the ``rgw keystone`` configuration options explained in :doc:`../keystone`,
* ``rgw s3 auth use keystone = true``.

In addition, a user wishing to use the S3 API must obtain an AWS-style
access key and secret key. They can do so with the ``openstack ec2
credentials create`` command::

  $ openstack --os-interface public ec2 credentials create
  +------------+---------------------------------------------------------------------------------------------------------------------------------------------+
  | Field      | Value                                                                                                                                       |
  +------------+---------------------------------------------------------------------------------------------------------------------------------------------+
  | access     | c921676aaabbccdeadbeef7e8b0eeb2c                                                                                                            |
  | links      | {u'self': u'https://auth.example.com:5000/v3/users/7ecbebaffeabbddeadbeefa23267ccbb24/credentials/OS-EC2/c921676aaabbccdeadbeef7e8b0eeb2c'} |
  | project_id | 5ed51981aab4679851adeadbeef6ebf7                                                                                                            |
  | secret     | ********************************                                                                                                            |
  | trust_id   | None                                                                                                                                        |
  | user_id    | 7ecbebaffeabbddeadbeefa23267cc24                                                                                                            |
  +------------+---------------------------------------------------------------------------------------------------------------------------------------------+

The thus-generated access and secret key can then be used for S3 API
access to radosgw.

.. note:: Consider that most production radosgw deployments
          authenticating against OpenStack Keystone are also set up
          for :doc:`../multitenancy`, for which special
          considerations apply with respect to S3 signed URLs and
          public read ACLs.

Access Control Lists (ACLs)
---------------------------

RGW supports S3-compatible ACL functionality. An ACL is a list of access grants
that specify which operations a user can perform on a bucket or on an object.
Each grant has a different meaning when applied to a bucket versus applied to 
an object:

+------------------+--------------------------------------------------------+----------------------------------------------+
| Permission       | Bucket                                                 | Object                                       |
+==================+========================================================+==============================================+
| ``READ``         | Grantee can list the objects in the bucket.            | Grantee can read the object.                 |
+------------------+--------------------------------------------------------+----------------------------------------------+
| ``WRITE``        | Grantee can write or delete objects in the bucket.     | N/A                                          |
+------------------+--------------------------------------------------------+----------------------------------------------+
| ``READ_ACP``     | Grantee can read bucket ACL.                           | Grantee can read the object ACL.             |
+------------------+--------------------------------------------------------+----------------------------------------------+
| ``WRITE_ACP``    | Grantee can write bucket ACL.                          | Grantee can write to the object ACL.         |
+------------------+--------------------------------------------------------+----------------------------------------------+
| ``FULL_CONTROL`` | Grantee has full permissions for object in the bucket. | Grantee can read or write to the object ACL. |
+------------------+--------------------------------------------------------+----------------------------------------------+

.. _RFC 2104: http://www.ietf.org/rfc/rfc2104.txt
.. _HMAC: https://en.wikipedia.org/wiki/HMAC
