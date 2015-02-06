====================
 Temp URL Operations
====================

To allow temporary access (for eg for `GET` requests) to objects
without the need to share credentials, temp url functionality is
supported by swift endpoint of radosgw. For this functionality,
initially the value of `X-Account-Meta-Temp-URL-Key` and optionally
`X-Account-Meta-Temp-URL-Key-2` should be set. The Temp URL
functionality relies on a HMAC-SHA1 signature against these secret
keys.

POST Temp-URL Keys
==================

A ``POST`` request to the swift account with the required Key will set
the secret temp url key for the account against which temporary url
access can be provided to accounts. Up to two keys are supported, and
signatures are checked against both the keys, if present, so that keys
can be rotated without invalidating the temporary urls.

Syntax
~~~~~~

::

	POST /{api version}/{account} HTTP/1.1
	Host: {fqdn}
	X-Auth-Token: {auth-token}

Request Headers
~~~~~~~~~~~~~~~

``X-Account-Meta-Temp-URL-Key``

:Description: A user-defined key that takes an arbitrary string value.
:Type: String
:Required: Yes

``X-Account-Meta-Temp-URL-Key-2``

:Description: A user-defined key that takes an arbitrary string value.
:Type: String
:Required: No


GET Temp-URL Objects
====================

Temporary URL uses a cryptographic HMAC-SHA1 signature, which includes
the following elements:

#. The value of the Request method, "GET" for instance
#. The expiry time, in format of seconds since the epoch, ie Unix time
#. The request path starting from "v1" onwards

The above items are normalized with newlines appended between them,
and a HMAC is generated using the SHA-1 hashing algorithm against one
of the Temp URL Keys posted earlier.

A sample python script to demonstrate the above is given below:


.. code-block:: python

   import hmac
   from hashlib import sha1
   from time import time

   method = 'GET'
   host = 'https://objectstore.example.com'
   duration_in_seconds = 300  # Duration for which the url is valid
   expires = int(time() + duration_in_seconds)
   path = '/v1/your-bucket/your-object'
   key = 'secret'
   hmac_body = '%s\n%s\n%s' % (method, expires, path)
   hmac_body = hmac.new(key, hmac_body, sha1).hexdigest()
   sig = hmac.new(key, hmac_body, sha1).hexdigest()
   rest_uri = "{host}{path}?temp_url_sig={sig}&temp_url_expires={expires}".format(
		host=host, path=path, sig=sig, expires=expires)
   print rest_uri

   # Example Output
   # https://objectstore.example.com/v1/your-bucket/your-object?temp_url_sig=ff4657876227fc6025f04fcf1e82818266d022c6&temp_url_expires=1423200992

