==================
PubSub Sync Module
==================

.. versionadded:: Nautilus

.. contents::

This sync module provides a publish and subscribe mechanism for the object store modification
events. Events are published into predefined topics. Topics can be subscribed to, and events
can be pulled from them. Events need to be acked. Also, events will expire and disappear
after a period of time. A push notification mechanism exists too, currently supporting HTTP and
AMQP0.9.1 endpoints.

A user can create different topics. A topic entity is defined by its user and its name. A
user can only manage its own topics, and can only subscribe to events published by buckets
it owns.

In order to publish events for specific bucket a notification needs to be created. A
notification can be created only on subset of event types, or for all event types (default).
There can be multiple notifications for any specific topic.

A subscription to a topic can also be defined. There can be multiple subscriptions for any
specific topic.

REST API has been defined to provide configuration and control interfaces for the pubsub
mechanisms. This API has two flavors, one is S3-compatible and one is not. The two flavors can be used
together, although it is recommended to use the S3-compatible one. 

Events are stored as RGW objects in a special bucket, under a special user. Events cannot
be accessed directly, but need to be pulled and acked using the new REST API.

.. toctree::
   :maxdepth: 1

   S3 Bucket Notification Compatibility <s3-notification-compatibility>

PubSub Zone Configuration
-------------------------

The pubsub sync module requires the creation of a new zone in a `Multisite`_ environment.
First, a master zone must exist (see: :ref:`master-zone-label`), 
then a secondary zone should be created (see :ref:`secondary-zone-label`).
In the creation of the secondary zone, its tier type must be set to ``pubsub``:

::

   # radosgw-admin zone create --rgw-zonegroup={zone-group-name} \
                               --rgw-zone={zone-name} \
                               --endpoints={http://fqdn}[,{http://fqdn}] \
                               --sync-from-all=0 \
                               --sync-from={master-zone-name} \
                               --tier-type=pubsub


PubSub Zone Configuration Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                                
::

   {
       "tenant": <tenant>,             # default: <empty>
       "uid": <uid>,                   # default: "pubsub"
       "data_bucket_prefix": <prefix>  # default: "pubsub-"
       "data_oid_prefix": <prefix>     #
       "events_retention_days": <days> # default: 7
   }

* ``tenant`` (string)

The tenant of the pubsub control user.

* ``uid`` (string)

The uid of the pubsub control user.

* ``data_bucket_prefix`` (string)

The prefix of the bucket name that will be created to store events for specific topic.

* ``data_oid_prefix`` (string)

The oid prefix for the stored events.

* ``events_retention_days`` (integer)

How many days to keep events that weren't acked.

Configuring Parameters via CLI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The tier configuration could be set using the following command:

::

   # radosgw-admin zone modify --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --tier-config={key}={val}[,{key}={val}]

Where the ``key`` in the configuration specifies the configuration variable that needs to be updated (from the list above), and
the ``val`` specifies its new value. For example, setting the pubsub control user ``uid`` to ``user_ps``:

::

   # radosgw-admin zone modify --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --tier-config=uid=pubsub

A configuration field can be removed by using ``--tier-config-rm={key}``.

PubSub Performance Stats
-------------------------
- ``pubsub_event_triggered``: running counter of events with at lease one pubsub topic associated with them
- ``pubsub_event_lost``: running counter of events that had pubsub topics and subscriptions associated with them but that were not stored or pushed to any of the subscriptions
- ``pubsub_store_ok``: running counter, for all subscriptions, of stored pubsub events 
- ``pubsub_store_fail``: running counter, for all subscriptions, of pubsub events that needed to be stored but failed
- ``pubsub_push_ok``: running counter, for all subscriptions, of pubsub events successfully pushed to their endpoint
- ``pubsub_push_fail``: running counter, for all subscriptions, of pubsub events failed to be pushed to their endpoint
- ``pubsub_push_pending``: gauge value of pubsub events pushed to a endpoint but not acked or nacked yet

.. note:: 

    ``pubsub_event_triggered`` and ``pubsub_event_lost`` are incremented per event, while: 
    ``pubsub_store_ok``, ``pubsub_store_fail``, ``pubsub_push_ok``, ``pubsub_push_fail``, are incremented per store/push action on each subscriptions.

PubSub REST API
---------------

.. tip:: PubSub REST calls, and only them, should be sent to an RGW which belong to a PubSub zone

Topics
~~~~~~

Create a Topic
``````````````

This will create a new topic. Topic creation is needed both for both flavors of the API.
Optionally the topic could be provided with push endpoint parameters that would be used later
when an S3-compatible notification is created.
Upon successful request, the response will include the topic ARN that could be later used to reference this topic in an S3-compatible notification request. 
To update a topic, use the same command used for topic creation, with the topic name of an existing topic and different endpoint values.

.. tip:: Any S3-compatible notification already associated with the topic needs to be re-created for the topic update to take effect 

::

   PUT /topics/<topic-name>[?push-endpoint=<endpoint>[&amqp-exchange=<exchange>][&amqp-ack-level=<level>][&verify-ssl=true|false]]

Request parameters:

- push-endpoint: URI of endpoint to send push notification to

 - URI schema is: ``http[s]|amqp://[<user>:<password>@]<fqdn>[:<port>][/<amqp-vhost>]``
 - Same schema is used for HTTP and AMQP endpoints (except amqp-vhost which is specific to AMQP)
 - Default values for HTTP/S: no user/password, port 80/443
 - Default values for AMQP: user/password=guest/guest, port 5672, amqp-vhost is "/"

- verify-ssl: can be used with https endpoints (ignored for other endpoints), indicate whether the server certificate is validated or not ("true" by default)
- amqp-exchange: mandatory parameter for AMQP endpoint. The exchanges must exist and be able to route messages based on topics
- amqp-ack-level: No end2end acking is required, as messages may persist in the broker before delivered into their final destination. 2 ack methods exist:

 - "none" - message is considered "delivered" if sent to broker
 - "broker" message is considered "delivered" if acked by broker

The topic ARN in the response will have the following format:

::

   arn:aws:sns:<zone-group>:<tenant>:<topic>

Get Topic Information
`````````````````````

Returns information about specific topic. This includes subscriptions to that topic, and push-endpoint information, if provided.

::

   GET /topics/<topic-name>

Response will have the following format (JSON):

::

   {
       "topic":{
           "user":"",
           "name":"",
           "dest":{
               "bucket_name":"",
               "oid_prefix":"",
               "push_endpoint":"",
               "push_endpoint_args":""
           },
           "arn":""
       },
       "subs":[]
   }             

- topic.user: name of the user that created the topic
- name: name of the topic
- dest.bucket_name: not used
- dest.oid_prefix: not used
- dest.push_endpoint: in case of S3-compliant notifications, this value will be used as the push-endpoint URL
- dest.push_endpoint_args: in case of S3-compliant notifications, this value will be used as the push-endpoint args
- topic.arn: topic ARN
- subs: list of subscriptions associated with this topic

Delete Topic
````````````

::

   DELETE /topics/<topic-name>

Delete the specified topic.

List Topics
```````````

List all topics that user defined.

::

   GET /topics

S3-Compliant Notifications
~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a Notification
`````````````````````

This will create a publisher for a specific bucket into a topic, and a subscription
for pushing/pulling events.
The subscription's name will have the same as the notification Id, and could be used later to fetch
and ack events with the subscription API.

::

   PUT /<bucket name>?notification

Request parameters are encoded in XML in the body of the request, with the following format:

::

   <NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <TopicConfiguration>
           <Id></Id>
           <Topic></Topic>
           <Event></Event>
       </TopicConfiguration>
   </NotificationConfiguration>

- Id: name of the notification
- Topic: topic ARN
- Event: either ``s3:ObjectCreated:*``, or ``s3:ObjectRemoved:*`` (multiple ``Event`` tags may be used)

Delete Notification
```````````````````

Delete a specific, or all, S3-compliant notifications from a bucket. Associated subscription will also be deleted.

::

   DELETE /bucket?notification[=<notification-id>]

Request parameters:

- notification-id: name of the notification (if not provided, all S3-compliant notifications on the bucket are deleted)

.. note:: 

    - Notification deletion is an extension to the S3 notification API
    - When the bucket is deleted, any notification defined on it is also deleted. 
      In this case, the associated subscription will not be deleted automatically (any events of the deleted bucket could still be access), 
      and will have to be deleted explicitly with the subscription deletion API

Get/List Notifications
``````````````````````

Get a specific S3-compliant notification, or list all S3-compliant notifications defined on a bucket.

::

   GET /bucket?notification[=<notification-id>]

Request parameters:

- notification-id: name of the notification (if not provided, all S3-compliant notifications on the bucket are listed)

Response is XML formatted:

::

   <NotificationConfiguration>
       <TopicConfiguration>
           <Id></Id>
           <Topic></Topic>
           <Event></Event>
       </TopicConfiguration>
   </NotificationConfiguration>

- Id: name of the notification
- Topic: topic ARN
- Event: either ``s3:ObjectCreated:*``, or ``s3:ObjectRemoved:*`` (multiple ``Event`` tags may be used)


.. note::

    - Getting information on a specific notification is an extension to the S3 notification API
    - When multiple notifications are fetched from the bucket, multiple ``NotificationConfiguration`` tags will be used

Non S3-Compliant Notifications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a Notification
`````````````````````

This will create a publisher for a specific bucket into a topic.

::

   PUT /notifications/bucket/<bucket>?topic=<topic-name>[&events=<event>[,<event>]]

Request parameters:

- topic-name: name of topic
- event: event type (string), one of: OBJECT_CREATE, OBJECT_DELETE 

Delete Notification Information
```````````````````````````````

Delete publisher from a specific bucket into a specific topic.

::

   DELETE /notifications/bucket/<bucket>?topic=<topic-name>

Request parameters:

- topic-name: name of topic

.. note:: When the bucket is deleted, any notification defined on it is also deleted

List Notifications
``````````````````

List all topics with associated events defined on a bucket.

::

   GET /notifications/bucket/<bucket>

Response will have the following format (JSON):

::

   {"topics":[
      {
         "topic":{
            "user":"",
            "name":"",
            "dest":{
               "bucket_name":"",
               "oid_prefix":"",
               "push_endpoint":"",
               "push_endpoint_args":""
            }
            "arn":""
         },
         "events":[]
      }
   ]}            

Subscriptions
~~~~~~~~~~~~~

Create a Subscription
`````````````````````

Creates a new subscription.

::

   PUT /subscriptions/<sub-name>?topic=<topic-name>[&push-endpoint=<endpoint>[&amqp-exchange=<exchange>][&amqp-ack-level=<level>][&verify-ssl=true|false]]

Request parameters:

- topic-name: name of topic
- push-endpoint: URI of endpoint to send push notification to

 - URI schema is: ``http[s]|amqp://[<user>:<password>@]<fqdn>[:<port>][/<amqp-vhost>]``
 - Same schema is used for HTTP and AMQP endpoints (except amqp-vhost which is specific to AMQP)
 - Default values for HTTP/S: no user/password, port 80/443
 - Default values for AMQP: user/password=guest/guest, port 5672, amqp-vhost is "/"

- verify-ssl: can be used with https endpoints (ignored for other endpoints), indicate whether the server certificate is validated or not ("true" by default)
- amqp-exchange: mandatory parameter for AMQP endpoint. The exchanges must exist and be able to route messages based on topics
- amqp-ack-level: No end2end acking is required, as messages may persist in the broker before delivered into their final destination. 2 ack methods exist:

 - "none": message is considered "delivered" if sent to broker
 - "broker": message is considered "delivered" if acked by broker

Get Subscription Information
````````````````````````````

Returns information about specific subscription.

::

   GET /subscriptions/<sub-name>

Response will have the following format (JSON):

::

   {
       "user":"",
       "name":"",
       "topic":"",
       "dest":{
           "bucket_name":"",
           "oid_prefix":"",
           "push_endpoint":"",
           "push_endpoint_args":""
       }
       "s3_id":""
   }             

- user: name of the user that created the subscription
- name: name of the subscription
- topic: name of the topic the subscription is associated with

Delete Subscription
```````````````````

Removes a subscription.

::

   DELETE /subscriptions/<sub-name>

Events
~~~~~~

Pull Events
```````````

Pull events sent to a specific subscription.

::

   GET /subscriptions/<sub-name>?events[&max-entries=<max-entries>][&marker=<marker>]

Request parameters:

- marker: pagination marker for list of events, if not specified will start from the oldest
- max-entries: max number of events to return

The response will hold information on the current marker and whether there are more events not fetched:

::

   {"next_marker":"","is_truncated":"",...}


The actual content of the response is depended with how the subscription was created.
In case that the subscription was created via an S3-compatible notification, 
the events will have an S3-compatible record format (JSON):

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
                   "arn":""
               },
               "object":{
                   "key":"",
                   "size": ,
                   "eTag":"",
                   "versionId":"",
                   "sequencer": ""
               }
           },
           "eventId":"",
       }
   ]}

- awsRegion: zonegroup
- eventTime: timestamp indicating when the event was triggered
- eventName: either ``s3:ObjectCreated:``, or ``s3:ObjectRemoved:``
- userIdentity: not supported 
- requestParameters: not supported
- responseElements: not supported
- s3.configurationId: notification ID that created the subscription for the event
- s3.eventId: unique ID of the event, that could be used for acking (an extension to the S3 notification API)
- s3.bucket.name: name of the bucket
- s3.bucket.ownerIdentity.principalId: owner of the bucket
- s3.bucket.arn: ARN of the bucket
- s3.object.key: object key
- s3.object.size: not supported
- s3.object.eTag: object etag
- s3.object.version: object version in case of versioned bucket
- s3.object.sequencer: monotonically increasing identifier of the change per object (hexadecimal format)

In case that the subscription was not created via a non S3-compatible notification, 
the events will have the following event format (JSON):

::

    {"events":[
       {
           "id":"",
           "event":"",
           "timestamp":"",
           "info":{
               "attrs":{
                   "mtime":""
               },
               "bucket":{
                   "bucket_id":"",
                   "name":"",
                   "tenant":""
               },
               "key":{
                   "instance":"",
                   "name":""
               }
           }
       }
   ]}

- id: unique ID of the event, that could be used for acking
- event: either ``OBJECT_CREATE``, or ``OBJECT_DELETE``
- timestamp: timestamp indicating when the event was sent
- info.attrs.mtime: timestamp indicating when the event was triggered
- info.bucket.bucket_id: id of the bucket
- info.bucket.name: name of the bucket
- info.bucket.tenant: tenant the bucket belongs to
- info.key.instance: object version in case of versioned bucket
- info.key.name: object key

Ack Event
`````````

Ack event so that it can be removed from the subscription history.

::

   POST /subscriptions/<sub-name>?ack&event-id=<event-id>

Request parameters:

- event-id: id of event to be acked

.. _Multisite : ../multisite
