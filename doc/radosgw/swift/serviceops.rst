====================
 Service Operations
====================

To retrieve data about our Swift-compatible service, you may execute ``GET`` 
requests using the ``X-Storage-Url`` value retrieved during authentication. 

List Containers
===============

A ``GET`` request that specifies the API version and the account will return
a list of containers for a particular user account. Since the request returns
a particular user's containers, the request requires an authentication token.
The request cannot be made anonymously.

Syntax
~~~~~~

::

	GET /{api version}/{account} HTTP/1.1
	Host: {fqdn}
	X-Auth-Token: {auth-token}



Request Parameters
~~~~~~~~~~~~~~~~~~

``limit``

:Description: Limits the number of results to the specified value.
:Type: Integer
:Required: No

``format``

:Description: Defines the format of the result. 
:Type: String
:Valid Values: ``json`` | ``xml``
:Required: No


``marker``

:Description: Returns a list of results greater than the marker value.
:Type: String
:Required: No



Response Entities
~~~~~~~~~~~~~~~~~

The response contains a list of containers, or returns with an HTTP
204 response code

``account``

:Description: A list for account information.
:Type: Container

``container``

:Description: The list of containers.
:Type: Container

``name``

:Description: The name of a container.
:Type: String

``bytes``

:Description: The size of the container.
:Type: Integer