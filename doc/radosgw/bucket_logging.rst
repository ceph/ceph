====================
Bucket Logging
====================

.. versionadded:: T

.. contents::

Bucket logging provides a mechanism for logging all access to a bucket. The
log data can be used to monitor bucket activity, detect unauthorized
access, get insights into the bucket usage and use the logs as a journal for bucket changes.
The log records are stored in objects in a separate bucket and can be analyzed later.
Logging configuration is done at the bucket level and can be enabled or disabled at any time.
The log bucket can accumulate logs from multiple buckets. It is recommended to configured 
a different "prefix" for each bucket, so that the logs of different buckets will be stored
in different objects in the log bucket.

.. note::

    - The log bucket must be created before enabling logging on a bucket
    - The log bucket cannot be the same as the bucket being logged
    - The log bucket cannot have logging enabled on it


.. toctree::
   :maxdepth: 1

Logging Reliability
-------------------
For performance reasons, even though the log records are written to persistent storage, the log object will
appear in the log bucket only after some configurable amount of time (or if the maximum object size of 128MB is reached).
This time (in seconds) could be set per source bucket via a Ceph extension to the REST API,
or globally via the `rgw_bucket_logging_obj_roll_time` configuration option. If not set, the default time is 5 minutes.
Adding a log object to the log bucket is done "lazily", meaning, that if no more records are written to the object, it may
remain outside of the log bucket even after the configured time has passed.
To counter that, you can flush all logging objects on a given source bucket to log them,
regardless if enough time passed or if no more records are written to the object.
Flushing will happen automatically when logging is disabled on a bucket, its logging configuration is changed, or the bucket is deleted.

Standard
````````
If logging type is set to "Standard" (the default) the log records are written to the log bucket after the bucket operation is completed.
This means that there are the logging operation may fail, with no indication to he client.

Journal
```````
If logging type is set to "Journal", the records are written to the log bucket before the bucket operation is completed. 
This means that if the logging action fails, the operation will not be executed, and an error will be returned to the client.
An exception to the above are "multi/delete" log records: if writing these log records fail, the operation continues and may still be successful.
Journal mode supports filtering out records based on matches of the prefixes and suffixes of the logged object keys. Regular-expression matching can also be used on these to create filters.
Note that it may happen that the log records were successfully written, but the bucket operation failed, since the logs are written.


Bucket Logging REST API
-----------------------
Detailed under: `Bucket Operations`_.


Log Objects Key Format
----------------------

Simple
``````
has the following format:

::

  <prefix><year-month-day-hour-minute-second>-<16 bytes unique-id>

For example:

::

  fish/2024-08-06-09-40-09-TI9ROKN05DD4HPQF

Partitioned
```````````
has the following format:

::

  <prefix><bucket owner>/<source region>/[tenant:]<bucket name>/<year>/<month>/<day>/<year-month-day-hour-minute-second>-<16 bytes unique-id>

For example:

::

  fish/testid//all-log/2024/08/06/2024-08-06-10-11-18-1HMU3UMWOJKNQJ0X

Log Records
~~~~~~~~~~~

The log records are space separated string columns and have the following possible formats:

Journal
```````
minimum amount of data used for journaling bucket changes (this is a Ceph extension).

  - bucket owner (or dash if empty)
  - bucket name (or dash if empty). in the format: ``[tenant:]<bucket name>``
  - time in the following format: ``[day/month/year:hour:minute:second timezone]``
  - object key (or dash if empty)
  - operation in the following format: ``WEBSITE/REST.<HTTP method>.<resource>``
  - object size (or dash if empty)
  - version id (dash if empty or question mark if unknown)
  - eTag

For example:

::

  testid fish [06/Aug/2024:09:40:09 +0000] myfile - REST.PUT.OBJECT 4cfdfc1f58e762d3e116787cb92fac60
  testid fish [06/Aug/2024:09:40:28 +0000] myfile REST.DELETE.OBJECT 4cfdfc1f58e762d3e116787cb92fac60


Standard
````````
based on `AWS Logging Record Format`_.

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
  - host id taken from "x-amz-id-2" (or dash if empty)
  - signature version (or dash if empty)
  - cipher suite (or dash if empty)
  - authentication type (or dash if empty)
  - host header (or dash if empty)
  - TLS version (or dash if empty)
  - access point ARN (not supported, always a dash)
  - ACL flag ("Yes" if the request is an ACL operation, otherwise dash)

For example:

::

  testid fish [06/Aug/2024:09:30:25 +0000] - testid 9e369a15-5f43-4f07-b638-de920b22f91b.4179.15085270386962380710 REST.PUT.OBJECT myfile "PUT /fish/myfile HTTP/1.1" 200 - 512 512 - - - - - - - - - localhost - -
  testid fish [06/Aug/2024:09:30:51 +0000] - testid 9e369a15-5f43-4f07-b638-de920b22f91b.4179.7046073853138417766 REST.GET.OBJECT myfile "GET /fish/myfile HTTP/1.1" 200 - - 512 - - - - - - - - - localhost - -
  testid fish [06/Aug/2024:09:30:56 +0000] - testid 9e369a15-5f43-4f07-b638-de920b22f91b.4179.10723158448701085570 REST.DELETE.OBJECT myfile "DELETE /fish/myfile1 HTTP/1.1" 200 - - 512 - - - - - - - - - localhost - -

 
.. _AWS Logging Record Format: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
.. _Bucket Operations: ../s3/bucketops
