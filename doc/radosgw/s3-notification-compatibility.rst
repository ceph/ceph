=====================================
S3 Bucket Notifications Compatibility
=====================================

Ceph's `Bucket Notifications`_ and `PubSub Module`_ APIs follow `AWS S3 Bucket Notifications API`_. However, some differences exist, as listed below.


.. note:: 

    Compatibility is different depending on which of the above mechanism is used

Supported Destination
---------------------

AWS supports: **SNS**, **SQS** and **Lambda** as possible destinations (AWS internal destinations). 
Currently, we support: **HTTP/S** and **AMQP**. And also support pulling and acking of events stored in Ceph (as an intenal destination).

We are using the **SNS** ARNs to represent the **HTTP/S** and **AMQP** destinations.

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

REST API Extension 
------------------

Ceph's bucket notification API has the following extensions:

- Deletion of a specific notification, or all notifications on a bucket, using the ``DELETE`` verb

 - In S3, all notifications are deleted when the bucket is deleted, or when an empty notification is set on the bucket

- Getting the information on a specific notification (when more than one exists on a bucket)

  - In S3, it is only possible to fetch all notifications on a bucket

- In addition to filtering based on prefix/suffix of object keys we support:

  - Filtering based on regular expression matching

  - Filtering based on metadata attributes attached to the object

  - Filtering based on object tags

- Filtering overlapping is allowed, so that same event could be sent as different notification


Unsupported Fields in the Event Record
--------------------------------------

The records sent for bucket notification follow format described in: `Event Message Structure`_.
However, the following fields may be sent empty, under the different deployment options (Notification/PubSub):

+----------------------------------------+--------------+---------------+------------------------------------------------------------+
| Field                                  | Notification | PubSub        | Description                                                |
+========================================+==============+===============+============================================================+
| ``userIdentity.principalId``           | Supported    | Not Supported | The identity of the user that triggered the event          |
+----------------------------------------+--------------+---------------+------------------------------------------------------------+
| ``requestParameters.sourceIPAddress``  |         Not Supported        | The IP address of the client that triggered the event      |
+----------------------------------------+--------------+---------------+------------------------------------------------------------+
| ``requestParameters.x-amz-request-id`` | Supported    | Not Supported | The request id that triggered the event                    |
+----------------------------------------+--------------+---------------+------------------------------------------------------------+
| ``requestParameters.x-amz-id-2``       | Supported    | Not Supported | The IP address of the RGW on which the event was triggered |
+----------------------------------------+--------------+---------------+------------------------------------------------------------+
| ``s3.object.size``                     | Supported    | Not Supported | The size of the object                                     |
+----------------------------------------+--------------+---------------+------------------------------------------------------------+

Event Types
-----------

+----------------------------------------------+-----------------+-------------------------------------------+
| Event                                        | Notification    | PubSub                                    |
+==============================================+=================+===========================================+
| ``s3:ObjectCreated:*``                       | Supported                                                   |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectCreated:Put``                     | Supported       | Supported at ``s3:ObjectCreated:*`` level |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectCreated:Post``                    | Supported       | Not Supported                             |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectCreated:Copy``                    | Supported       | Supported at ``s3:ObjectCreated:*`` level |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectCreated:CompleteMultipartUpload`` | Supported       | Supported at ``s3:ObjectCreated:*`` level |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectRemoved:*``                       | Supported       | Supported only the specific events below  |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectRemoved:Delete``                  | Supported                                                   |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectRemoved:DeleteMarkerCreated``     | Supported                                                   |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectRestore:Post``                    | Not applicable to Ceph                                      |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ObjectRestore:Complete``                | Not applicable to Ceph                                      |
+----------------------------------------------+-----------------+-------------------------------------------+
| ``s3:ReducedRedundancyLostObject``           | Not applicable to Ceph                                      |
+----------------------------------------------+-----------------+-------------------------------------------+

Topic Configuration
-------------------
In the case of bucket notifications, the topics management API will be derived from `AWS Simple Notification Service API`_. 
Note that most of the API is not applicable to Ceph, and only the following actions are implemented:

 - ``CreateTopic``
 - ``DeleteTopic``
 - ``ListTopics``

We also extend it by: 

 - ``GetTopic`` - allowing for fetching a specific topic, instead of all user topics
 - In ``CreateTopic`` we allow setting endpoint attributes

.. _AWS Simple Notification Service API: https://docs.aws.amazon.com/sns/latest/api/API_Operations.html
.. _AWS S3 Bucket Notifications API: https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
.. _Event Message Structure: https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
.. _`PubSub Module`: ../pubsub-module
.. _`Bucket Notifications`: ../notifications
