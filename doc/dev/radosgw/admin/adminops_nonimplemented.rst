==================
 Admin Operations
==================

An admin API request will be done on a URI that starts with the configurable 'admin'
resource entry point. Authorization for the admin API duplicates the S3 authorization
mechanism. Some operations require that the user holds special administrative capabilities.
The response entity type (XML or JSON) may be specified as the 'format' option in the
request and defaults to JSON if not specified.

Get Object
==========

Get an existing object. NOTE: Does not require owner to be non-suspended.

Syntax
~~~~~~

::

	GET /{admin}/bucket?object&format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket containing the object to be retrieved.
:Type: String
:Example: ``foo_bucket``
:Required: Yes

``object``

:Description: The object to be retrieved.
:Type: String
:Example: ``foo.txt``
:Required: Yes

Response Entities
~~~~~~~~~~~~~~~~~

If successful, returns the desired object.

``object``

:Description: The desired object.
:Type: Object

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``NoSuchObject``

:Description: Specified object does not exist.
:Code: 404 Not Found

Head Object
===========

Verify the existence of an object. If the object exists,
metadata headers for the object will be returned.

Syntax
~~~~~~

::

	HEAD /{admin}/bucket?object HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

``bucket``

:Description: The bucket containing the object to be retrieved.
:Type: String
:Example: ``foo_bucket``
:Required: Yes

``object``

:Description: The object to be retrieved.
:Type: String
:Example: ``foo.txt``
:Required: Yes

Response Entities
~~~~~~~~~~~~~~~~~

None.

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

``NoSuchObject``

:Description: Specified object does not exist.
:Code: 404 Not Found

Get Zone Info
=============

Get cluster information.

Syntax
~~~~~~

::

	GET /{admin}/zone&format=json HTTP/1.1
	Host {fqdn}


Response Entities
~~~~~~~~~~~~~~~~~

If successful, returns cluster pool configuration.

``zone``

:Description: Contains current cluster pool configuration.
:Type: Container

``domain_root``

:Description: root of all buckets.
:Type: String
:Parent: ``cluster``

``control_pool``

:Description: 
:Type: String
:Parent: ``cluster``

``gc_pool``

:Description: Garbage collection pool.
:Type: String
:Parent: ``cluster``

``log_pool``

:Description: Log pool.
:Type: String
:Parent: ``cluster``

``intent_log_pool``

:Description: Intent log pool.
:Type: String
:Parent: ``cluster``

``usage_log_pool``

:Description: Usage log pool.
:Type: String
:Parent: ``cluster``

``user_keys_pool``

:Description: User key pool.
:Type: String
:Parent: ``cluster``

``user_email_pool``

:Description: User email pool.
:Type: String
:Parent: ``cluster``

``user_swift_pool``

:Description: Pool of swift users.
:Type: String
:Parent: ``cluster``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

None.

Example Response
~~~~~~~~~~~~~~~~

::

	HTTP/1.1 200
	Content-Type: application/json

	{
	  "domain_root": ".rgw",
	  "control_pool": ".rgw.control",
	  "gc_pool": ".rgw.gc",
	  "log_pool": ".log",
	  "intent_log_pool": ".intent-log",
	  "usage_log_pool": ".usage",
	  "user_keys_pool": ".users",
	  "user_email_pool": ".users.email",
	  "user_swift_pool": ".users.swift",
	  "user_uid_pool ": ".users.uid"
	}



Add Placement Pool
==================

Make a pool available for data placement.

Syntax
~~~~~~

::

	PUT /{admin}/pool?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``pool``

:Description: The pool to be made available for data placement.
:Type: String
:Example: ``foo_pool``
:Required: Yes

``create``

:Description: Creates the data pool if it does not exist.
:Type: Boolean
:Example: False [False]
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

TBD.

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

Remove Placement Pool
=====================

Make a pool unavailable for data placement.

Syntax
~~~~~~

::

	DELETE /{admin}/pool?format=json HTTP/1.1
	Host {fqdn}


Request Parameters
~~~~~~~~~~~~~~~~~~

``pool``

:Description: The existing pool to be made available for data placement.
:Type: String
:Example: ``foo_pool``
:Required: Yes

``destroy``

:Description: Destroys the pool after removing it from the active set.
:Type: Boolean
:Example: False [False]
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

TBD.

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

List Available Data Placement Pools
===================================

List current pools available for data placement.

Syntax
~~~~~~

::

	GET /{admin}/pool?format=json HTTP/1.1
	Host {fqdn}


Response Entities
~~~~~~~~~~~~~~~~~

If successful, returns a list of pools available for data placement.

``pools``

:Description: Contains currently available pools for data placement.
:Type: Container



List Expired Garbage Collection Items
=====================================

List objects scheduled for garbage collection.

Syntax
~~~~~~

::

	GET /{admin}/garbage?format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

None.

Response Entities
~~~~~~~~~~~~~~~~~

If expired garbage collection items exist, a list of such objects
will be returned.

``garbage``

:Description: Expired garbage collection items.
:Type: Container

``object``

:Description: A container garbage collection object information.
:Type: Container
:Parent: ``garbage``

``name``

:Description: The name of the object.
:Type: String
:Parent: ``object``

``expired``

:Description: The date at which the object expired.
:Type: String
:Parent: ``object``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

Manually Processes Garbage Collection Items
===========================================

List objects scheduled for garbage collection.

Syntax
~~~~~~

::

	DELETE /{admin}/garbage?format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

None.

Response Entities
~~~~~~~~~~~~~~~~~

If expired garbage collection items exist, a list of removed objects
will be returned.

``garbage``

:Description: Expired garbage collection items.
:Type: Container

``object``

:Description: A container garbage collection object information.
:Type: Container
:Parent: ``garbage``

``name``

:Description: The name of the object.
:Type: String
:Parent: ``object``

``expired``

:Description: The date at which the object expired.
:Type: String
:Parent: ``object``

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

TBD.

Show Log Objects
================

Show log objects

Syntax
~~~~~~

::

	GET /{admin}/log?format=json HTTP/1.1
	Host {fqdn}

Request Parameters
~~~~~~~~~~~~~~~~~~

``object``

:Description: The log object to return.
:Type: String:
:Example: ``2012-10-11-09-4165.2-foo_bucket``
:Required: No

Response Entities
~~~~~~~~~~~~~~~~~

If no object is specified, returns the full list of log objects.

``log-objects``

:Description: A list of log objects.
:Type: Container

``object``

:Description: The name of the log object.
:Type: String

``log``

:Description: The contents of the log object.
:Type: Container

Special Error Responses
~~~~~~~~~~~~~~~~~~~~~~~

None.

Standard Error Responses
========================

``AccessDenied``

:Description: Access denied.
:Code: 403 Forbidden

``InternalError``

:Description: Internal server error.
:Code: 500 Internal Server Error

``NoSuchUser``

:Description: User does not exist.
:Code: 404 Not Found

``NoSuchBucket``

:Description: Bucket does not exist.
:Code: 404 Not Found

``NoSuchKey``

:Description: No such access key.
:Code: 404 Not Found
