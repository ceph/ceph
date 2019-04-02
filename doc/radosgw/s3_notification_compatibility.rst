=====================================
S3 Bucket Notifications Compatibility
=====================================

Notification Push Endpoints
---------------------------

AWS supports internal endpoints: SNS, SQS and Lambda as possible push endpoints. Currently we support HTTP/S and AMQP.
We are using the SNS ARNs to represent these endpoints.
We also support pulling and acking of events as described in the `pubsub module`_ documentation.

Notification configuration XML
------------------------------

Following tags (and the tags inside them) are not supported:

- ``<QueueConfiguration>``: this is used for AWS SQS endpoints, we treat all endpoints as SNS
- ``<CloudFunctionConfiguration>``: this is used for AWS Lambda endpoints, we treat all endpoints as SNS
- ``<Filter>``: object filtering based on key name/prefix/suffix not supported

REST API Extension
------------------

- We support deletion of a specific notification, or all notifications on a bucket, without deletion of the bucket
- We support getting the information on a specific notification (when some exists on a bucket)  

Unsupported Fields in the Event Record
--------------------------------------

The records sent for bucket notification follow the S3 format described in the `pubsub module`_ documentation.
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

.. _`pubsub module`: ./pubsub-module.rst

