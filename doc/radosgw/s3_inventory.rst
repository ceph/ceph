================
Bucket Inventory
================

.. versionadded:: Umbrella

.. contents::

Bucket inventory produces a periodic list of the objects in a bucket, with
some of their metadata, as an alternative to listing the bucket through the
API. The report is written as a `Parquet`_ file into a destination bucket,
together with a ``manifest.json`` that describes it, using the same layout as
`AWS S3 Inventory`_. Tools that read AWS inventory reports can read these as
well.

A bucket can have up to 1000 inventory configurations. Each one has its own
destination, schedule, version filter and list of fields.

.. note::

    - Inventory is only available when ``radosgw`` is built with Arrow/Parquet
      support (``WITH_RADOSGW_SELECT_PARQUET``). Otherwise the inventory API is
      not available.
    - Only the ``Parquet`` format is supported. ``CSV`` and ``ORC`` are rejected.
    - The destination bucket must exist in the same cluster. It cannot be the
      source bucket.
    - The destination bucket must have a bucket policy that allows delivery (see
      `Destination Bucket Policy`_).
    - Reports are not encrypted. A configuration with a ``Filter`` or an
      ``Encryption`` element is rejected.


Configuration
-------------

The configuration is set with the `PutBucketInventoryConfiguration`_ API. For
example, with the AWS CLI:

.. prompt:: bash $

   aws s3api put-bucket-inventory-configuration --bucket mybucket --id daily \
     --inventory-configuration file://inventory.json

where ``inventory.json`` contains:

.. code-block:: json

   {
     "Id": "daily",
     "IsEnabled": true,
     "IncludedObjectVersions": "All",
     "Schedule": {"Frequency": "Daily"},
     "Destination": {
       "S3BucketDestination": {
         "Bucket": "arn:aws:s3:::inventory-bucket",
         "Format": "Parquet",
         "Prefix": "reports"
       }
     },
     "OptionalFields": ["Size", "LastModifiedDate", "ETag", "StorageClass"]
   }

To use a destination bucket of another tenant, use ``tenant:bucket`` as the
bucket name in the ARN (e.g. ``arn:aws:s3:::tenant2:inventory-bucket``).

The other configuration APIs are supported as well:

- `GetBucketInventoryConfiguration`_
- `ListBucketInventoryConfigurations`_ (all configurations are returned in one
  response)
- `DeleteBucketInventoryConfiguration`_

Setting, changing or deleting a configuration requires the
``s3:PutInventoryConfiguration`` permission on the source bucket. Reading or
listing them requires ``s3:GetInventoryConfiguration``.


Destination Bucket Policy
-------------------------

Reports are written to the destination bucket as the ``s3.amazonaws.com``
service principal, so the destination bucket must have a policy that allows
this principal to put objects. The policy is checked when the configuration
is set and again before every report. For example:

.. code-block:: json

   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Principal": {"Service": "s3.amazonaws.com"},
       "Action": "s3:PutObject",
       "Resource": "arn:aws:s3:::inventory-bucket/*",
       "Condition": {
         "ArnLike": {"aws:SourceArn": "arn:aws:s3:::mybucket"},
         "StringEquals": {"aws:SourceAccount": "<owner of mybucket>"}
       }
     }]
   }

The conditions are optional. ``aws:SourceArn`` limits delivery to reports of
the given source buckets, and ``aws:SourceAccount`` to source buckets owned by
the given user or account.


Report Contents
---------------

Each row in the report describes one object, or one object version if
``IncludedObjectVersions`` is ``All``. Rows are ordered by key. The ``bucket``
and ``key`` columns are always present. With ``All``, the ``version_id``,
``is_latest`` and ``is_delete_marker`` columns are added. Objects that were
written before versioning was enabled on the bucket have the version id
``null``. Delete markers have no size, ETag or storage class.

The following fields can be selected in ``OptionalFields``:

+----------------------------------+----------------------------------------+
| Field                            | Remarks                                |
+==================================+========================================+
| ``Size``                         |                                        |
+----------------------------------+----------------------------------------+
| ``LastModifiedDate``             |                                        |
+----------------------------------+----------------------------------------+
| ``ETag``                         |                                        |
+----------------------------------+----------------------------------------+
| ``StorageClass``                 |                                        |
+----------------------------------+----------------------------------------+
| ``IsMultipartUploaded``          |                                        |
+----------------------------------+----------------------------------------+
| ``ReplicationStatus``            | Accepted, the column is always empty   |
+----------------------------------+----------------------------------------+
| ``EncryptionStatus``             | Accepted, the column is always empty   |
+----------------------------------+----------------------------------------+
| ``ObjectLockRetainUntilDate``    | Accepted, the column is always empty   |
+----------------------------------+----------------------------------------+
| ``ObjectLockMode``               | Accepted, the column is always empty   |
+----------------------------------+----------------------------------------+
| ``ObjectLockLegalHoldStatus``    | Accepted, the column is always empty   |
+----------------------------------+----------------------------------------+
| ``IntelligentTieringAccessTier`` | Accepted, the column is always empty   |
+----------------------------------+----------------------------------------+
| ``BucketKeyStatus``              | Accepted, the column is always empty   |
+----------------------------------+----------------------------------------+

The fields that are always empty are not stored in the bucket index, and
reading them would need a request per object.


Report Layout
-------------

Reports are written under ``<prefix>/<source bucket>/<configuration id>/`` in
the destination bucket (without the leading ``<prefix>/`` if no prefix is
set). For a source bucket in a non-default tenant, ``<source bucket>`` is
``<tenant>:<bucket>``.

- ``data/<uuid>.parquet``: the report
- ``<YYYY-MM-DD>T00-00Z/manifest.json``: the list of report files, their size
  and MD5 checksum, and the Parquet schema of the report
- ``<YYYY-MM-DD>T00-00Z/manifest.checksum``: the MD5 of ``manifest.json``

``manifest.json`` is written last, so a report is complete once its manifest
exists.


Schedule
--------

Reports are generated by the lifecycle worker, which visits each bucket once a
day (see ``rgw_lifecycle_work_time``). A ``Daily`` configuration produces one
report per day (UTC). A ``Weekly`` configuration produces one report a week,
on a day of the week that is derived from the source bucket's name. If a day's
report already exists, it is not generated again on that day.

Buckets without inventory configurations are not affected.

For testing, ``rgw_lc_debug_interval`` shortens the lifecycle cycle, and a
lifecycle pass (including inventory) can be started manually with:

.. prompt:: bash #

   radosgw-admin lc process --bucket <source bucket>


.. _Parquet: https://parquet.apache.org/
.. _AWS S3 Inventory: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html
.. _PutBucketInventoryConfiguration: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketInventoryConfiguration.html
.. _GetBucketInventoryConfiguration: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketInventoryConfiguration.html
.. _ListBucketInventoryConfigurations: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketInventoryConfigurations.html
.. _DeleteBucketInventoryConfiguration: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketInventoryConfiguration.html
