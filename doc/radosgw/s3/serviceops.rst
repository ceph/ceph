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

Gets usage statistics for the authenticated user, similar to the admin
command :ref:`rgw_user_usage_stats`.

The response combines three kinds of data:

- **Storage** — current bytes and object counts (``Summary``, ``CapacityUsed``)
- **Operations** — per-category ops and bandwidth (``Entries``), only when
  usage logging is enabled on the cluster

Usage Logging
~~~~~~~~~~~~~

Operation statistics in ``Entries`` require the following setting in the
appropriate ``ceph.conf`` section, followed by an ``radosgw`` restart::

  rgw enable usage log = true

Without this setting, ``Entries`` is empty but ``CapacityUsed`` and storage
fields in ``Summary`` are still returned.

Syntax
~~~~~~
::

	GET /?usage HTTP/1.1
	Host: cname.domain.com

	Authorization: AWS {access-key}:{hash-of-header-and-secret}

Parameters
~~~~~~~~~~

+--------------+----------------------------------------------------------+---------------+------------+
| Name         | Description                                              | Example       | Required   |
+==============+==========================================================+===============+============+
| ``start-date`` | Start of the usage log time range (inclusive).         | ``2024-01-01``| No         |
+--------------+----------------------------------------------------------+---------------+------------+
| ``end-date``   | End of the usage log time range.                       | ``2025-01-01``| No         |
+--------------+----------------------------------------------------------+---------------+------------+

Response Entities
~~~~~~~~~~~~~~~~~

+----------------------------+-------------+-----------------------------------------------------------------+
| Name                       | Type        | Description                                                     |
+============================+=============+=================================================================+
| ``Entries``                | Container   | Usage log entries (ops and bandwidth per bucket, per time       |
|                            |             | window). Empty when usage logging is disabled.                  |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``Summary``                | Container   | Aggregated quota settings and total storage for the user.       |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``CapacityUsed``           | Container   | Current storage usage broken down per bucket.                   |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``TotalBytes``             | Integer     | Total bytes used by the user across all buckets.                |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``TotalBytesRounded``      | Integer     | Total bytes rounded to the nearest 4 KiB boundary.              |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``TotalEntries``           | Integer     | Total number of object entries for the user.                    |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``Bucket``                 | String      | Bucket name (inside ``CapacityUsed``).                          |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``Bytes``                  | Integer     | Raw bytes stored in the bucket.                                 |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``Bytes_Rounded``          | Integer     | Bytes stored, rounded to the nearest 4 KiB boundary.            |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``Category``               | String      | Operation category (for example ``put_obj``, ``get_obj``,       |
|                            |             | ``delete_obj``) inside ``Entries``.                             |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``Ops``                    | Integer     | Number of operations in the category.                           |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``BytesSent``              | Integer     | Bytes sent in response to operations.                           |
+----------------------------+-------------+-----------------------------------------------------------------+
| ``BytesReceived``          | Integer     | Bytes received in request bodies.                               |
+----------------------------+-------------+-----------------------------------------------------------------+
