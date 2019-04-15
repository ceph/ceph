=====================================
S3 Bucket Notifications Compatibility
=====================================

Ceph's `PubSub module`_ follows `AWS S3 Bucket Notifications API`_. However, some differences exist, as listed below.

Supported Destination
----------------------

AWS supports: **SNS**, **SQS** and **Lambda** as possible destinations (AWS internal destinations). 
Currently, we support: **HTTP/S** and **AMQP**. And also support pulling and acking of events stored in Ceph (as an intenal destination).

We are using the SNS ARNs to represent these destinations.

Notification Configuration XML
------------------------------

Following tags (and the tags inside them) are not supported:

+-----------------------------------+----------------------------------------------+
| Tag                               | Remaks                                       |
+===================================+==============================================+
| ``<QueueConfiguration>``          | not needed, we treat all destinations as SNS |
+-----------------------------------+----------------------------------------------+
| ``<CloudFunctionConfiguration>``  | not needed, we treat all destinations as SNS |
+-----------------------------------+----------------------------------------------+
| ``<Filter>``                      | object filtering not supported               |
+-----------------------------------+----------------------------------------------+

REST API Extension
------------------

Ceph's bucket notification API follows has the following extensions:

- Deletion of a specific notification, or all notifications on a bucket, using the ``DELETE`` verb

 - In S3, all notifications are deleted when the bucket is deleted, or when an empty notification is set on the bucket

- Getting the information on a specific notification (when some exists on a bucket)  

Unsupported Fields in the Event Record
--------------------------------------

The records sent for bucket notification follow format described in: `Event Message Structure`_.
However, the following fields are sent empty:

+----------------------------------------+-------------------------------------------------------------+
| Field                                  | Description                                                 |
+========================================+=============================================================+
| ``userIdentity.principalId``           | The identity of the user that triggered the event           |
+----------------------------------------+-------------------------------------------------------------+
| ``requestParameters.sourceIPAddress``  | The IP address of the client that triggered the event       |
+----------------------------------------+-------------------------------------------------------------+
| ``requestParameters.x-amz-request-id`` | The request id that triggered the event                     |
+----------------------------------------+-------------------------------------------------------------+
| ``requestParameters.x-amz-id-2``       | The IP address of the RGW on which the event was triggered  |
+----------------------------------------+-------------------------------------------------------------+
| ``s3.object.size``                     | The size of the object                                      |
+----------------------------------------+-------------------------------------------------------------+

Event Types
-----------

+----------------------------------------------+-----------------+-------------------------------------------+
| Event                                        | Status          | Remarks                                   |
+==============================================+=================+===========================================+
| ``s3:ObjectCreated:*``                       | Supported       |                                           |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectCreated:Put``                     | Supported       | supported at ``s3:ObjectCreated:*`` level |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectCreated:Post``                    | Not Supported   | start of multi-part upload not supported  |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectCreated:Copy``                    | Supported       | supported at ``s3:ObjectCreated:*`` level |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectCreated:CompleteMultipartUpload`` | Supported       | supported at ``s3:ObjectCreated:*`` level |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectRemoved:*``                       | Supported       |                                           |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectRemoved:Delete``                  | Supported       | supported at ``s3:ObjectRemoved:*`` level |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectRemoved:DeleteMarkerCreated``     | Supported       | supported at ``s3:ObjectRemoved:*`` level |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectRestore:Post``                    | Not Supported   | not applicable to Ceph                    |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectRestore:Complete``                | Not Supported   | not applicable to Ceph                    |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ReducedRedundancyLostObject``           | Not Supported   | not applicable to Ceph                    |
+----------------------------------------------+-----------------+-------------------------------------------+

.. _AWS S3 Bucket Notifications API: https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
.. _Event Message Structure: https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
.. _`PubSub module`: ../pubsub-module
