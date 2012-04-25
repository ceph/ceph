Authentication and ACLs
=======================
Requests to the RADOS Gateway (RGW) can be either authenticated or unauthenticated. 
RGW assumes unauthenticated requests are sent by an anonymous user. RGW supports 
canned ACLs. 

Authentication
--------------

Authenticating a request requires including an access key and a Hash-based Message Authentication Code (HMAC)
in the request before it is sent to the RGW server. RGW uses an S3-compatible authentication
approach. The HTTP header signing is similar to OAuth 1.0, but avoids the complexity associated with the 3-legged OAuth 1.0 method.

::

	HTTP/1.1
	PUT /buckets/bucket/object.mpeg
	Host: cname.domain.com
	Date: Mon, 2 Jan 2012 00:01:01 +0000
	Content-Length: 9999999
	Content-Encoding: mpeg

	Authorization: AWS {access-key}:{hash-of-header-and-secret}

In the foregoing example, replace ``{access-key}`` with the value for your access key ID followed by 
a colon (``:``). Replace ``{hash-of-header-and-secret}`` with a hash of the header string and the secret
corresponding to the access key ID.

To generate the hash of the header string and secret, you must:

1. Get the value of the header string and the secret::

	str = "HTTP/1.1\nPUT /buckets/bucket/object.mpeg\nHost: cname.domain.com\n
	Date: Mon, 2 Jan 2012 00:01:01 +0000\nContent-Length: 9999999\nContent-Encoding: mpeg";
	
	secret = "valueOfSecret";

2. Generate an HMAC using a SHA-1 hashing algorithm. ::
   
    hmac = object.hmac-sha1(str, secret);
   
3. Encode the ``hmac`` result using base-64. ::

    encodedHmac = someBase64Encoder.encode(hmac);

Replace the ``{hash-of-header-and-secret}`` with the base-64 encoded HMAC string.

Access Control Lists (ACLs)
---------------------------

RGW supports S3-compatible ACL functionality. An ACL is a list of access grants
that specify which operations a user can perform on a bucket or on an object.
Each grant has a different meaning when applied to a bucket versus applied to an object:

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