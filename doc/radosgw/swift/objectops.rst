===================
 Object Operations
===================

An object is a container for storing data and metadata. A container may
have many objects, but the object names must be unique. This API enables a 
client to create an object, set access controls and metadata, retrieve an 
object's data and metadata, and delete an object. Since this API makes requests 
related to information in a particular user's account, all requests in this API 
must be authenticated unless the container or object's access control is 
deliberately made publicly accessible (i.e., allows anonymous requests).


Create/Update an Object
=======================

To create a new object, make a ``PUT`` request with the API version, account,
container name and the name of the new object. You must have write permission
on the container to create or update an object. The object name must be 
unique within the container. The ``PUT`` request is not idempotent, so if you
do not use a unique name, the request will update the object. However, you may
use pseudo-hierarchical syntax in your object name to distinguish it from 
another object of the same name if it is under a different pseudo-hierarchical 
directory. You may include access control headers and metadata headers in the 
request.


Syntax
~~~~~~

::

   PUT /{api version}/{account}/{container}/{object} HTTP/1.1 
	Host: {fqdn}
	X-Auth-Token: {auth-token}


Request Headers
~~~~~~~~~~~~~~~

``ETag``

:Description: An MD5 hash of the object's contents. Recommended. 
:Type: String
:Required: No


``Content-Type``

:Description: The type of content the object contains.
:Type: String
:Required: No


``Transfer-Encoding``

:Description: Indicates whether the object is part of a larger aggregate object.
:Type: String
:Valid Values: ``chunked``
:Required: No


Copy an Object
==============

Copying an object allows you to make a server-side copy of an object, so that
you don't have to download it and upload it under another container/name.
To copy the contents of one object to another object, you may make either a
``PUT`` request or a ``COPY`` request with the API version, account, and the 
container name. For a ``PUT`` request, use the destination container and object
name in the request, and the source container and object in the request header.
For a ``Copy`` request, use the source container and object in the request, and
the destination container and object in the request header. You must have write 
permission on the container to copy an object. The destination object name must be 
unique within the container. The request is not idempotent, so if you do not use 
a unique name, the request will update the destination object. However, you may 
use pseudo-hierarchical syntax in your object name to distinguish the destination 
object from the source object of the same name if it is under a different 
pseudo-hierarchical directory. You may include access control headers and metadata 
headers in the request.

Syntax
~~~~~~

::

	PUT /{api version}/{account}/{dest-container}/{dest-object} HTTP/1.1
	X-Copy-From: {source-container}/{source-object}
	Host: {fqdn}
	X-Auth-Token: {auth-token}


or alternatively:

::

	COPY /{api version}/{account}/{source-container}/{source-object} HTTP/1.1
	Destination: {dest-container}/{dest-object}

Request Headers
~~~~~~~~~~~~~~~

``X-Copy-From``

:Description: Used with a ``PUT`` request to define the source container/object path.
:Type: String
:Required: Yes, if using ``PUT``


``Destination``

:Description: Used with a ``COPY`` request to define the destination container/object path.
:Type: String
:Required: Yes, if using ``COPY``


``If-Modified-Since``

:Description: Only copies if modified since the date/time of the source object's ``last_modified`` attribute.
:Type: Date
:Required: No


``If-Unmodified-Since``

:Description: Only copies if not modified since the date/time of the source object's ``last_modified`` attribute.
:Type: Date
:Required: No

``Copy-If-Match``

:Description: Copies only if the ETag in the request matches the source object's ETag.
:Type: ETag.
:Required: No


``Copy-If-None-Match``

:Description: Copies only if the ETag in the request does not match the source object's ETag.
:Type: ETag.
:Required: No


Delete an Object
================

To delete an object, make a ``DELETE`` request with the API version, account,
container and object name. You must have write permissions on the container to delete
an object within it. Once you've successfully deleted the object, you'll be able to 
reuse the object name.

Syntax
~~~~~~

::

	DELETE /{api version}/{account}/{container}/{object} HTTP/1.1
	Host: {fqdn}
	X-Auth-Token: {auth-token}


Get an Object
=============

To retrieve an object, make a ``GET`` request with the API version, account,
container and object name. You must have read permissions on the container to
retrieve an object within it.

Syntax
~~~~~~

::

	GET /{api version}/{account}/{container}/{object} HTTP/1.1
	Host: {fqdn}
	X-Auth-Token: {auth-token}



Request Headers
~~~~~~~~~~~~~~~

``range``

:Description: To retrieve a subset of an object's contents, you may specify a byte range.
:Type: Date
:Required: No


``If-Modified-Since``

:Description: Only copies if modified since the date/time of the source object's ``last_modified`` attribute.
:Type: Date
:Required: No


``If-Unmodified-Since``

:Description: Only copies if not modified since the date/time of the source object's ``last_modified`` attribute.
:Type: Date
:Required: No

``Copy-If-Match``

:Description: Copies only if the ETag in the request matches the source object's ETag.
:Type: ETag.
:Required: No


``Copy-If-None-Match``

:Description: Copies only if the ETag in the request does not match the source object's ETag.
:Type: ETag.
:Required: No



Response Headers
~~~~~~~~~~~~~~~~

``Content-Range``

:Description: The range of the subset of object contents. Returned only if the range header field was specified in the request  


Get Object Metadata
===================

To retrieve an object's metadata, make a ``HEAD`` request with the API version, 
account, container and object name. You must have read permissions on the 
container to retrieve metadata from an object within the container. This request
returns the same header information as the request for the object itself, but
it does not return the object's data.

Syntax
~~~~~~

::

	HEAD /{api version}/{account}/{container}/{object} HTTP/1.1
	Host: {fqdn}
	X-Auth-Token: {auth-token}



Add/Update Object Metadata
==========================

To add metadata to an object, make a ``POST`` request with the API version, 
account, container and object name. You must have write permissions on the 
parent container to add or update metadata.


Syntax
~~~~~~

::

	POST /{api version}/{account}/{container}/{object} HTTP/1.1
	Host: {fqdn}
	X-Auth-Token: {auth-token}

Request Headers
~~~~~~~~~~~~~~~

``X-Object-Meta-{key}``

:Description:  A user-defined meta data key that takes an arbitrary string value.
:Type: String
:Required: No

