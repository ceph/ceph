Service Operations
==================

List Buckets
------------
``GET /`` returns a list of buckets created by the user making the request. ``GET /`` only
returns buckets created by an authenticated user. You cannot make an anonymous request.

Syntax
~~~~~~
::

	GET / HTTP/1.1
	Host: cname.domain.com

	Authorization: AWS {access-key}:{hash-of-header-and-secret}

Response Entities
~~~~~~~~~~~~~~~~~

+----------------------------+-------------+-----------------------------------------------------------------+
| Name                       | Type        | Description                                                     |
+============================+=============+=================================================================+
| ``Buckets``                | Container   | Container for list of buckets.                                  |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``Bucket``                 | Container   | Container for bucket information.                               |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``Name``                   | String      | Bucket name.                                                    |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``CreationDate``           | Date        | UTC time when the bucket was created.                           |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``ListAllMyBucketsResult`` | Container   | A container for the result.                                     |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``Owner``                  | Container   | A container for the bucket owner's ``ID`` and ``DisplayName``.  |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``ID``                     | String      | The bucket owner's ID.                                          |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``DisplayName``            | String      | The bucket owner's display name.                                |
+----------------------------+-------------+-----------------------------------------------------------------+


Get Usage Stats
---------------

Gets usage stats per user, similar to the admin command :ref:`rgw_user_usage_stats`.

Syntax
~~~~~~
::

	GET /?usage HTTP/1.1
	Host: cname.domain.com

	Authorization: AWS {access-key}:{hash-of-header-and-secret}

Response Entities
~~~~~~~~~~~~~~~~~

+----------------------------+-------------+-----------------------------------------------------------------+
| Name                       | Type        | Description                                                     |
+============================+=============+=================================================================+
| ``Summary``                | Container   | Summary of total stats by user.                                 |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``TotalBytes``             | Integer     | Bytes used by user                                              |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``TotalBytesRounded``      | Integer     | Bytes rounded to the nearest 4k boundary                        |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``TotalEntries``           | Integer     | Total object entries                                            |
+----------------------------+-------------+-----------------------------------------------------------------+
