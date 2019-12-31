====================
Bucket Notifications
====================

.. versionadded:: Nautilus

.. contents::

Bucket notifications provide a mechanism for sending information out of the radosgw when certain events are happening on the bucket.
Currently, notifications could be sent to: HTTP, AMQP0.9.1 and Kafka endpoints.

Note, that if the events should be stored in Ceph, in addition, or instead of being pushed to an endpoint, 
the `PubSub Module`_ should be used instead of the bucket notification mechanism.

A user can create different topics. A topic entity is defined by its user and its name. A
user can only manage its own topics, and can only associate them with buckets it owns.

In order to send notifications for events for a specific bucket, a notification entity needs to be created. A
notification can be created on a subset of event types, or for all event types (default).
The notification may also filter out events based on prefix/suffix and/or regular expression matching of the keys. As well as,
on the metadata attributes attached to the object.
There can be multiple notifications for any specific topic, and the same topic could be used for multiple notifications.

REST API has been defined to provide configuration and control interfaces for the bucket notification
mechanism. This API is similar to the one defined as the S3-compatible API of the pubsub sync module.

.. toctree::
   :maxdepth: 1

   S3 Bucket Notification Compatibility <s3-notification-compatibility>

Notification Performance Stats
------------------------------
The same counters are shared between the pubsub sync module and the bucket notification mechanism.

- ``pubsub_event_triggered``: running counter of events with at least one topic associated with them
- ``pubsub_event_lost``: running counter of events that had topics associated with them but that were not pushed to any of the endpoints
- ``pubsub_push_ok``: running counter, for all notifications, of events successfully pushed to their endpoint
- ``pubsub_push_fail``: running counter, for all notifications, of events failed to be pushed to their endpoint
- ``pubsub_push_pending``: gauge value of events pushed to an endpoint but not acked or nacked yet

.. note:: 

    ``pubsub_event_triggered`` and ``pubsub_event_lost`` are incremented per event, while: 
    ``pubsub_push_ok``, ``pubsub_push_fail``, are incremented per push action on each notification.

Bucket Notification REST API
----------------------------

Topics
~~~~~~

Create a Topic
``````````````

This will create a new topic. The topic should be provided with push endpoint parameters that would be used later
when a notification is created.
Upon a successful request, the response will include the topic ARN that could be later used to reference this topic in the notification request.
To update a topic, use the same command used for topic creation, with the topic name of an existing topic and different endpoint values.

.. tip:: Any notification already associated with the topic needs to be re-created for the topic update to take effect 

::

   POST
   Action=CreateTopic
   &Name=<topic-name>
   &push-endpoint=<endpoint>
   [&Attributes.entry.1.key=amqp-exchange&Attributes.entry.1.value=<exchange>]
   [&Attributes.entry.2.key=amqp-ack-level&Attributes.entry.2.value=none|broker]
   [&Attributes.entry.3.key=verify-sll&Attributes.entry.3.value=true|false]
   [&Attributes.entry.4.key=kafka-ack-level&Attributes.entry.4.value=none|broker]

Request parameters:

- push-endpoint: URI of an endpoint to send push notification to
- HTTP endpoint 

 - URI: ``http[s]://<fqdn>[:<port]``
 - port defaults to: 80/443 for HTTP/S accordingly
 - verify-ssl: indicate whether the server certificate is validated by the client or not ("true" by default)

- AMQP0.9.1 endpoint

 - URI: ``amqp://[<user>:<password>@]<fqdn>[:<port>][/<vhost>]``
 - user/password defaults to : guest/guest
 - port defaults to: 5672
 - vhost defaults to: "/"
 - amqp-exchange: the exchanges must exist and be able to route messages based on topics (mandatory parameter for AMQP0.9.1)
 - amqp-ack-level: no end2end acking is required, as messages may persist in the broker before delivered into their final destination. Two ack methods exist:

  - "none": message is considered "delivered" if sent to broker
  - "broker": message is considered "delivered" if acked by broker (default)

- Kafka endpoint 

 - URI: ``kafka://<fqdn>[:<port]``
 - port defaults to: 9092
 - kafka-ack-level: no end2end acking is required, as messages may persist in the broker before delivered into their final destination. Two ack methods exist:

  - "none": message is considered "delivered" if sent to broker
  - "broker": message is considered "delivered" if acked by broker (default)

.. note:: 

    - The key/value of a specific parameter does not have to reside in the same line, or in any specific order, but must use the same index
    - Attribute indexing does not need to be sequential or start from any specific value
    - `AWS Create Topic`_ has a detailed explanation of the endpoint attributes format. However, in our case different keys and values are used

The response will have the following format:

::

    <CreateTopicResponse xmlns="https://sns.amazonaws.com/doc/2010-03-31/">
        <CreateTopicResult>
            <TopicArn></TopicArn>
        </CreateTopicResult>
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </CreateTopicResponse>    

The topic ARN in the response will have the following format:

::

   arn:aws:sns:<zone-group>:<tenant>:<topic>
 
Get Topic Information
`````````````````````

Returns information about specific topic. This includes push-endpoint information, if provided.

::

   POST
   Action=GetTopic&TopicArn=<topic-arn>

Response will have the following format:

::

    <GetTopicResponse>
        <GetTopicRersult>
            <Topic>
                <User></User>
                <Name></Name>
                <EndPoint>
                    <EndpointAddress></EndpointAddress>
                    <EndpointArgs></EndpointArgs>
                    <EndpointTopic></EndpointTopic>
                </EndPoint>
                <TopicArn></TopicArn>
            </Topic>
        </GetTopicResult>
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </GetTopicResponse>    

- User: name of the user that created the topic
- Name: name of the topic
- EndPoinjtAddress: the push-endpoint URL
- EndPointArgs: the push-endpoint args
- EndpointTopic: the topic name that should be sent to the endpoint (mat be different than the above topic name)
- TopicArn: topic ARN

Delete Topic
````````````

::

   POST
   Action=DeleteTopic&TopicArn=<topic-arn>

Delete the specified topic. Note that deleting a deleted topic should result with no-op and not a failure.

The response will have the following format:

::

    <DeleteTopicResponse xmlns="https://sns.amazonaws.com/doc/2010-03-31/">
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </DeleteTopicResponse>    

List Topics
```````````

List all topics that user defined.

::

   POST 
   Action=ListTopics
 
Response will have the following format:

::

    <ListTopicdResponse xmlns="https://sns.amazonaws.com/doc/2010-03-31/">
        <ListTopicsRersult>
            <Topics>
                <member>
                    <User></User>
                    <Name></Name>
                    <EndPoint>
                        <EndpointAddress></EndpointAddress>
                        <EndpointArgs></EndpointArgs>
                        <EndpointTopic></EndpointTopic>
                    </EndPoint>
                    <TopicArn></TopicArn>
                </member>
            </Topics>
        </ListTopicsResult>
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </ListTopicsResponse>    

Notifications
~~~~~~~~~~~~~

Detailed under: `Bucket Operations`_.

.. note:: 

    - "Abort Multipart Upload" request does not emit a notification
    - "Delete Multiple Objects" request does not emit a notification
    - Both "Initiate Multipart Upload" and "POST Object" requests will emit an ``s3:ObjectCreated:Post`` notification


Events
~~~~~~

The events are in JSON format (regardless of the actual endpoint), and share the same structure as the S3-compatible events
pushed or pulled using the pubsub sync module.

::

   {"Records":[  
       {
           "eventVersion":"2.1"
           "eventSource":"aws:s3",
           "awsRegion":"",
           "eventTime":"",
           "eventName":"",
           "userIdentity":{  
               "principalId":""
           },
           "requestParameters":{
               "sourceIPAddress":""
           },
           "responseElements":{
               "x-amz-request-id":"",
               "x-amz-id-2":""
           },
           "s3":{
               "s3SchemaVersion":"1.0",
               "configurationId":"",
               "bucket":{
                   "name":"",
                   "ownerIdentity":{
                       "principalId":""
                   },
                   "arn":"",
                   "id:""
               },
               "object":{
                   "key":"",
                   "size":"",
                   "eTag":"",
                   "versionId":"",
                   "sequencer": "",
                   "metadata":[]
               }
           },
           "eventId":"",
       }
   ]}

- awsRegion: zonegroup
- eventTime: timestamp indicating when the event was triggered
- eventName: for list of supported events see: `S3 Notification Compatibility`_
- userIdentity.principalId: user that triggered the change
- requestParameters.sourceIPAddress: not supported
- responseElements.x-amz-request-id: request ID of the original change 
- responseElements.x_amz_id_2: RGW on which the change was made 
- s3.configurationId: notification ID that created the event
- s3.bucket.name: name of the bucket
- s3.bucket.ownerIdentity.principalId: owner of the bucket
- s3.bucket.arn: ARN of the bucket
- s3.bucket.id: Id of the bucket (an extension to the S3 notification API)
- s3.object.key: object key
- s3.object.size: object size
- s3.object.eTag: object etag
- s3.object.version: object version in case of versioned bucket
- s3.object.sequencer: monotonically increasing identifier of the change per object (hexadecimal format)
- s3.object.metadata: any metadata set on the object sent as: ``x-amz-meta-`` (an extension to the S3 notification API) 
- s3.eventId: unique ID of the event, that could be used for acking (an extension to the S3 notification API)

.. _PubSub Module : ../pubsub-module
.. _S3 Notification Compatibility: ../s3-notification-compatibility
.. _AWS Create Topic: https://docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html
.. _Bucket Operations: ../s3/bucketops
