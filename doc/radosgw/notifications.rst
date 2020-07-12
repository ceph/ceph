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
on the metadata attributes attached to the object, or the object tags.
There can be multiple notifications for any specific topic, and the same topic could be used for multiple notifications.

REST API has been defined to provide configuration and control interfaces for the bucket notification
mechanism. This API is similar to the one defined as the S3-compatible API of the pubsub sync module.

.. toctree::
   :maxdepth: 1

   S3 Bucket Notification Compatibility <s3-notification-compatibility>


Topic Management via CLI
------------------------

Configuration of all topics of a user could be fetched using the following command:
   
::
   
   # radosgw-admin topic list --uid={user-id}


Configuration of a specific topic could be fetched using:

::
   
   # radosgw-admin topic get --uid={user-id} --topic={topic-name}


And removed using:

::
   
   # radosgw-admin topic rm --uid={user-id} --topic={topic-name}


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

.. note:: 

    In all topic actions, the parameters are URL encoded, and sent in the message body using ``application/x-www-form-urlencoded`` content type

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
   [&Attributes.entry.1.key=amqp-exchange&Attributes.entry.1.value=<exchange>]
   [&Attributes.entry.2.key=amqp-ack-level&Attributes.entry.2.value=none|broker|routable]
   [&Attributes.entry.3.key=verify-ssl&Attributes.entry.3.value=true|false]
   [&Attributes.entry.4.key=kafka-ack-level&Attributes.entry.4.value=none|broker]
   [&Attributes.entry.5.key=use-ssl&Attributes.entry.5.value=true|false]
   [&Attributes.entry.6.key=ca-location&Attributes.entry.6.value=<file path>]
   [&Attributes.entry.7.key=OpaqueData&Attributes.entry.7.value=<opaque data>]
   [&Attributes.entry.8.key=push-endpoint&Attributes.entry.8.value=<endpoint>]

Request parameters:

- push-endpoint: URI of an endpoint to send push notification to
- OpaqueData: opaque data is set in the topic configuration and added to all notifications triggered by the topic

- HTTP endpoint 

 - URI: ``http[s]://<fqdn>[:<port]``
 - port defaults to: 80/443 for HTTP/S accordingly
 - verify-ssl: indicate whether the server certificate is validated by the client or not ("true" by default)

- AMQP0.9.1 endpoint

 - URI: ``amqp://[<user>:<password>@]<fqdn>[:<port>][/<vhost>]``
 - user/password defaults to: guest/guest
 - user/password may only be provided over HTTPS. Topic creation request will be rejected if not
 - port defaults to: 5672
 - vhost defaults to: "/"
 - amqp-exchange: the exchanges must exist and be able to route messages based on topics (mandatory parameter for AMQP0.9.1). Different topics pointing to the same endpoint must use the same exchange
 - amqp-ack-level: no end2end acking is required, as messages may persist in the broker before delivered into their final destination. Three ack methods exist:

  - "none": message is considered "delivered" if sent to broker
  - "broker": message is considered "delivered" if acked by broker (default)
  - "routable": message is considered "delivered" if broker can route to a consumer

.. tip:: The topic-name (see :ref:`radosgw-create-a-topic`) is used for the AMQP topic ("routing key" for a topic exchange)

- Kafka endpoint 

 - URI: ``kafka://[<user>:<password>@]<fqdn>[:<port]``
 - if ``use-ssl`` is set to "true", secure connection will be used for connecting with the broker ("false" by default)
 - if ``ca-location`` is provided, and secure connection is used, the specified CA will be used, instead of the default one, to authenticate the broker
 - user/password may only be provided over HTTPS. Topic creation request will be rejected if not
 - user/password may only be provided together with ``use-ssl``, connection to the broker would fail if not
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

   Action=GetTopic
   &TopicArn=<topic-arn>

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
                <OpaqueData></OpaqueData>
            </Topic>
        </GetTopicResult>
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </GetTopicResponse>    

- User: name of the user that created the topic
- Name: name of the topic
- EndPoinjtAddress: the push-endpoint URL
- if endpoint URL contain user/password information, request must be made over HTTPS. Topic get request will be rejected if not 
- EndPointArgs: the push-endpoint args
- EndpointTopic: the topic name that should be sent to the endpoint (mat be different than the above topic name)
- TopicArn: topic ARN

Delete Topic
````````````

::

   POST

   Action=DeleteTopic
   &TopicArn=<topic-arn>

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
                    <OpaqueData></OpaqueData>
                </member>
            </Topics>
        </ListTopicsResult>
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </ListTopicsResponse>    

- if endpoint URL contain user/password information, in any of the topic, request must be made over HTTPS. Topic list request will be rejected if not 

Notifications
~~~~~~~~~~~~~

Detailed under: `Bucket Operations`_.

.. note:: 

    - "Abort Multipart Upload" request does not emit a notification
    - Both "Initiate Multipart Upload" and "POST Object" requests will emit an ``s3:ObjectCreated:Post`` notification


Events
~~~~~~

The events are in JSON format (regardless of the actual endpoint), and share the same structure as the S3-compatible events
pushed or pulled using the pubsub sync module. For example:

::

   {"Records":[  
       {
           "eventVersion":"2.1",
           "eventSource":"ceph:s3",
           "awsRegion":"us-east-1",
           "eventTime":"2019-11-22T13:47:35.124724Z",
           "eventName":"s3:ObjectCreated:Put",
           "userIdentity":{  
               "principalId":"tester"
           },
           "requestParameters":{
               "sourceIPAddress":""
           },
           "responseElements":{
               "x-amz-request-id":"503a4c37-85eb-47cd-8681-2817e80b4281.5330.903595",
               "x-amz-id-2":"14d2-zone1-zonegroup1"
           },
           "s3":{
               "s3SchemaVersion":"1.0",
               "configurationId":"mynotif1",
               "bucket":{
                   "name":"mybucket1",
                   "ownerIdentity":{
                       "principalId":"tester"
                   },
                   "arn":"arn:aws:s3:us-east-1::mybucket1",
                   "id":"503a4c37-85eb-47cd-8681-2817e80b4281.5332.38"
               },
               "object":{
                   "key":"myimage1.jpg",
                   "size":"1024",
                   "eTag":"37b51d194a7513e45b56f6524f2d51f2",
                   "versionId":"",
                   "sequencer": "F7E6D75DC742D108",
                   "metadata":[],
                   "tags":[]
               }
           },
           "eventId":"",
           "opaqueData":"me@example.com"
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
- s3.object.tags: any tags set on the object (an extension to the S3 notification API)
- s3.eventId: unique ID of the event, that could be used for acking (an extension to the S3 notification API)
- s3.opaqueData: opaque data is set in the topic configuration and added to all notifications triggered by the topic (an extension to the S3 notification API)

.. _PubSub Module : ../pubsub-module
.. _S3 Notification Compatibility: ../s3-notification-compatibility
.. _AWS Create Topic: https://docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html
.. _Bucket Operations: ../s3/bucketops
