==============
Bucket Logging
==============

.. versionadded:: T

.. contents::

Bucket logging provides a mechanism for logging all access to a bucket. The log
data can be used to monitor bucket activity, detect unauthorized access, get
insights into bucket usage, and use the logs as a journal for bucket
changes. The log records are stored in objects in a separate bucket and can be
analyzed later. Logging configuration is done at the bucket level and can be
enabled or disabled at any time. The log bucket can accumulate logs from
multiple buckets. It is recommended to configure a different "prefix" for each
bucket, so that the logs of different buckets will be stored in different
objects in the log bucket.

.. note::

    - The log bucket must be created before enabling logging on a bucket.
    - The log bucket cannot be the same as the bucket being logged.
    - The log bucket cannot have logging enabled on it.
    - The log bucket cannot have any encryption set on it (including SSE-S3
      with AES-256).
    - The log bucket cannot have any compression set on it.
    - The log bucket must not have RequestPayer enabled.
    - Source and log buckets must be in the same zonegroup.
    - Source and log buckets may belong to different accounts (with proper
      bucket policy set).
    - The log bucket may have object lock enabled with default retention period.
    - The 16-byte unique ID part of the log object name is a lexicographically
      ordered random string that consists of a 10-byte counter and a 6-byte
      random alphanumeric string (or a random alphanumeric string if the
      counter is not available).


.. toctree::
   :maxdepth: 1

Logging Reliability
-------------------
For performance reasons, even though the log records are written to persistent
storage, the log object will appear in the log bucket only after some
configurable amount of time (or if the maximum object size of 128MB is
reached). This time (in seconds) can be set per source bucket via a Ceph
extension to the :ref:`REST API <radosgw s3>`, or globally via the
``rgw_bucket_logging_obj_roll_time`` configuration option. If not set, the
default time is 5 minutes. Adding a log object to the log bucket is done
"lazily", meaning that if no more records are written to the object, it may
remain outside of the log bucket even after the configured time has passed. To
counter that, you can flush all logging objects on a given source bucket to log
them, regardless if enough time passed or if no more records are written to the
object. Flushing will happen automatically when logging is disabled on a
bucket, or its logging configuration is changed, or the bucket is deleted.

Standard
````````
If the logging type is set to "Standard" (the default) the log records are
written to the log bucket after the bucket operation is completed. This means
that the logging operation may fail with no indication to the client.

Journal
```````
If the logging type is set to "Journal", the records are written to the log
bucket before the bucket operation is completed. This means that if the logging
action fails, the operation is not executed and an error is returned to the
client. Some exceptions to that rule exist: the "Fails Operation" columns in
the table below indicate by "No" which operations will not fail even if logging
failed. Journal mode supports filtering out records based on matches of the
prefixes and suffixes of the logged object keys. Regular expression matching
can also be used on these to create filters. Note that it may happen that the
log records were successfully written but the bucket operation failed, since
the logs are written.

The following operations are supported in journal mode:

+-------------------------------+-------------------------------------+-----------------+
| Operation                     | Operation Name                      | Fails Operation |
+===============================+=====================================+=================+
| ``PutObject``                 | ``REST.PUT.OBJECT``                 | Yes             |
+-------------------------------+-------------------------------------+-----------------+
| ``DeleteObject``              | ``REST.DELETE.OBJECT``              | No              |
+-------------------------------+-------------------------------------+-----------------+
| ``DeleteObjects``             | ``REST.POST.DELETE_MULTI_OBJECT``   | No              |
+-------------------------------+-------------------------------------+-----------------+
| ``CompleteMultipartUpload``   | ``REST.POST.UPLOAD``                | Yes             |
+-------------------------------+-------------------------------------+-----------------+
| ``CopyObject``                | ``REST.PUT.OBJECT``                 | Yes             |
+-------------------------------+-------------------------------------+-----------------+
| ``PutObjectAcl``              | ``REST.PUT.ACL``                    | Yes             |
+-------------------------------+-------------------------------------+-----------------+
| ``PutObjectLegalHold``        | ``REST.PUT.LEGAL_HOLD``             | Yes             |
+-------------------------------+-------------------------------------+-----------------+
| ``PutObjectRetention``        | ``REST.PUT.RETENTION``              | Yes             |
+-------------------------------+-------------------------------------+-----------------+
| ``PutObjectTagging``          | ``REST.PUT.OBJECT_TAGGING``         | Yes             |
+-------------------------------+-------------------------------------+-----------------+
| ``DeleteObjectTagging``       | ``REST.DELETE.OBJECT_TAGGING``      | No              |
+-------------------------------+-------------------------------------+-----------------+

Multisite
`````````
In a multi-zone deployment, each zone uses its own log object before the
log object is added to the log bucket. After the log object is added to the
log bucket (that is, after being flushed) it is replicated to other zones.
This means that for a given time period there can be more than one log object
holding relevant log records.

Bucket Logging Policy
---------------------
Only the owner of the source bucket is allowed to enable or disable bucket
logging. For a bucket to be used as a log bucket, it must have a bucket policy
that allows that (even if the source bucket and the log bucket are owned by the
same user or account). The bucket policy must allow the ``s3:PutObject`` action
for the log bucket, to be performed by the ``logging.s3.amazonaws.com`` service
principal. The bucket policy should also specify the source bucket and account
that are expected to write logs to it. For example:

::

    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Sid": "AllowLoggingFromSourceBucket",
          "Effect": "Allow",
          "Principal": {
            "Service": "logging.s3.amazonaws.com"
          },
          "Action": "s3:PutObject",
          "Resource": "arn:aws:s3:::log-bucket-name/prefix*",
          "Condition": {
            "StringEquals": {
              "aws:SourceAccount": "source-account-id"
            },
            "ArnLike": {
              "aws:SourceArn": "arn:aws:s3:::source-bucket-name"
            }
          }
        }
      ]
    }


Bucket Logging Quota
--------------------
Bucket and user quota are applied on the log bucket. Quota is checked every
time a log record is written, and is updated when the log object is added to
the log bucket. In "Journal" mode, if the quota is exceeded, the logging
operation fails and as a result the bucket operation also fails. In "Standard"
mode, the logging operation is skipped, but the bucket operation continues.


Bucket Logging REST API
-----------------------
Detailed under: `Bucket Operations`_.


Log Objects Key Format
----------------------

Simple
``````
The "Simple" log objects key has the following format:

::

  <prefix><year-month-day-hour-minute-second>-<16 bytes unique-id>

For example:

::

  fish/2024-08-06-09-40-09-0000000002AGQ6W1

Partitioned
```````````
The "Partitioned" log objects key has the following format:

::

  <prefix><source bucket owner>/<zone group>/[tenant:]<source bucket name>/<year>/<month>/<day>/<year-month-day-hour-minute-second>-<16 bytes unique-id>

For example:

::

  fish/testid/default/fish-bucket/2024/08/06/2024-08-06-10-11-18-0000000011D1FGPA

Log Records Format
------------------

The log records are space-separated string columns and have the following
possible formats:

Journal
```````
The "Journal" record format uses minimum amount of data for journaling
bucket changes (this is a Ceph extension).

  - bucket owner (or dash if empty)
  - bucket name (or dash if empty), in the format: ``[tenant:]<bucket name>``
  - time in the following format: ``[day/month/year:hour:minute:second timezone]``
  - operation in the following format: ``WEBSITE/REST.<HTTP method>.<resource>``
  - object key (or dash if empty)
  - object size (or dash if empty)
  - version id (or dash if empty)
  - eTag (or dash if empty)

For example:

::

  testid fish [06/Aug/2024:09:40:09 +0000] REST.PUT.OBJECT myfile - 512 4cfdfc1f58e762d3e116787cb92fac60
  testid fish [06/Aug/2024:09:40:28 +0000] REST.DELETE.OBJECT myfile - - 4cfdfc1f58e762d3e116787cb92fac60


Standard
````````
The "Standard" record format is based on `AWS Logging Record Format`_.

  - bucket owner (or dash if empty)
  - bucket name (or dash if empty) in the format: ``[tenant:]<bucket name>``
  - time in the following format: ``[day/month/year:hour:minute:second timezone]`` where "timezone" is in UTC offset
  - client IP address (or dash if empty)
  - user or account (or dash if empty)
  - request ID
  - operation in the following format: ``WEBSITE/REST.<HTTP method>.<resource>``
  - object key (or dash if empty)
  - request URI in the following format: ``"<HTTP method> <URI> <HTTP version>"``
  - HTTP status (or dash if zero). Note that in most cases log is written before the status is known
  - error code (or dash if empty)
  - bytes sent (or dash if zero)
  - object size (or dash if zero)
  - total time (not supported, always a dash)
  - turnaround time in milliseconds
  - referer (or dash if empty)
  - user agent (or dash if empty) inside double quotes
  - version id (or dash if empty)
  - host id taken from ``x-amz-id-2`` (or dash if empty)
  - signature version (or dash if empty)
  - cipher suite (or dash if empty)
  - authentication type (``AuthHeader`` for regular auth, ``QueryString`` for presigned URL or dash if unauthenticated)
  - host header (or dash if empty)
  - TLS version (or dash if empty)
  - access point ARN (not supported, always a dash)
  - ACL flag (``Yes`` if an ACL was required for authorization, otherwise dash)

For example:

::

  testid fish [06/Aug/2024:09:30:25 +0000] - testid 9e369a15-5f43-4f07-b638-de920b22f91b.4179.15085270386962380710 REST.PUT.OBJECT myfile "PUT /fish/myfile HTTP/1.1" 200 - 512 512 - - - - - - - - - localhost - -
  testid fish [06/Aug/2024:09:30:51 +0000] - testid 9e369a15-5f43-4f07-b638-de920b22f91b.4179.7046073853138417766 REST.GET.OBJECT myfile "GET /fish/myfile HTTP/1.1" 200 - - 512 - - - - - - - - - localhost - -
  testid fish [06/Aug/2024:09:30:56 +0000] - testid 9e369a15-5f43-4f07-b638-de920b22f91b.4179.10723158448701085570 REST.DELETE.OBJECT myfile "DELETE /fish/myfile1 HTTP/1.1" 200 - - 512 - - - - - - - - - localhost - -


.. _AWS Logging Record Format: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
.. _Bucket Operations: ../s3/bucketops
