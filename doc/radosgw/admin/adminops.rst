====================
 Admin Operations
====================

An admin API request will be done on a URI that starts with the configurable 'admin'
resource entry point. Authorization for the admin API duplicates the S3 authorization
mechanism. Some operations require that the user holds special administrative capabilities.

Get Usage
===============

Request usage information.

Syntax
~~~~~~

::

	GET /{admin}/usage HTTP/1.1
	Host: {fqdn}



Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user for which the information is requested. If not specified will apply to all users.
:Type: String
:Required: No

``start``

:Description: Date and (optional) time that specifies the start time of the requested data.
:Type: String
:Example: ``2012-09-25 16:00:00``
:Required: No

``end``

:Description: Date and (optional) time that specifies the end time of the requested data (non-inclusive)
:Type: String
:Example: ``2012-09-25 16:00:00``
:Required: No


``show-entries``

:Description: Specifies whether data entries should be returned
:Type: Boolean
:Required: No


``show-summary``

:Description: Specifies whether data summary should be returned
:Type: Boolean
:Required: No



Response Entities
~~~~~~~~~~~~~~~~~

If successful, the response contains the requested information.

``usage``

:Description: A container for the usage information
:Type: Container

``entries``

:Description: A container for the usage entries information
:Type: Container

``user``

:Description: A container for the user data information
:Type: Container

``owner``

:Description: The name of the user that owns the buckets
:Type: String

``bucket``

:Description: The bucket name
:Type: String

``time``

:Description: Time lower bound for which data is being specified (rounded to the beginning of the first relevant hour).
:Type: String

``epoch``

:Description: The time specified in seconds since 1/1/1970.
:Type: String

``categories``

:Description: A container for stats categories
:Type: Container

``entry``

:Description: A container for stats entry
:Type: Container

``category``

:Description: Name of request category for which the stats are provided
:Type: String

``bytes_sent``

:Description: Number of bytes sent by the RADOS Gateway
:Type: Integer

``bytes_received``

:Description: Number of bytes received by the RADOS Gateway
:Type: Integer

``ops``

:Description: Number of operations
:Type: Integer

``successful_ops``

:Description: Number of successful operations
:Type: Integer

``summary``

:Description: A container for stats summary
:Type: Container

``total``

:Description: A container for stats summary aggregated total
:Type: Container


Trim Usage
===============

Remove usage information.

Syntax
~~~~~~

::

	DELETE /{admin}/usage HTTP/1.1
	Host: {fqdn}



Request Parameters
~~~~~~~~~~~~~~~~~~

``uid``

:Description: The user for which the information is requested. If not specified will apply to all users.
:Type: String
:Required: No

``start``

:Description: Date and (optional) time that specifies the start time of the requested data.
:Type: String
:Example: ``2012-09-25 16:00:00``
:Required: No

``start``

:Description: Date and (optional) time that specifies the end time of the requested data (none inclusive)
:Type: String
:Example: ``2012-09-25 16:00:00``
:Required: No


``remove-all``

:Description: Required when uid is not specified, in order to acknowledge multi user data removal.
:Type: Boolean
:Required: No


