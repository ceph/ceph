====================
Bucket Notifications
====================

.. versionadded:: Nautilus

.. versionchanged:: Squid
   A new "v2" format for Topic and Notification metadata can be enabled with
   the :ref:`feature_notification_v2` zone feature.

.. contents::

Bucket notifications provide a mechanism for sending information out of radosgw
when certain events happen on the bucket. Notifications can be sent to HTTP
endpoints, AMQP0.9.1 endpoints, and Kafka endpoints.

A user can create topics. A topic entity is defined by its name and is "per
tenant". A user can associate its topics (via notification configuration) only
with buckets it owns.

A notification entity must be created in order to send event notifications for
a specific bucket. A notification entity can be created either for a subset
of event types or for all "Removed" and "Created" event types (which is the default). The
notification may also filter out events based on matches of the prefixes and
suffixes of (1) the keys, (2) the metadata attributes attached to the object,
or (3) the object tags. Regular-expression matching can also be used on these
to create filters. There can be multiple notifications for any specific topic,
and the same topic can used for multiple notifications.

REST API has been defined so as to provide configuration and control interfaces
for the bucket notification mechanism.

.. toctree::
   :maxdepth: 1

   S3 Bucket Notification Compatibility <s3-notification-compatibility>

.. note:: To enable bucket notifications API, the `rgw_enable_apis` configuration parameter should contain: "notifications".

Notification Reliability
------------------------

Notifications can be sent synchronously or asynchronously. This section
describes the latency and reliability that you should expect for synchronous
and asynchronous notifications.

Synchronous Notifications
~~~~~~~~~~~~~~~~~~~~~~~~~

Notifications can be sent synchronously, as part of the operation that
triggered them. In this mode, the operation is acknowledged (acked) only after
the notification is sent to the topic's configured endpoint. This means that
the round trip time of the notification (the time it takes to send the
notification to the topic's endpoint plus the time it takes to receive the
acknowledgement) is added to the latency of the operation itself.

.. note:: The original triggering operation is considered successful even if
   the notification fails with an error, cannot be delivered, or times out.

Asynchronous Notifications
~~~~~~~~~~~~~~~~~~~~~~~~~~

Notifications can be sent asynchronously. They are committed into persistent
storage and then asynchronously sent to the topic's configured endpoint. In
this case, the only latency added to the original operation is the latency
added when the notification is committed to persistent storage.

.. note:: If the notification fails with an error, cannot be delivered, or
   times out, it is retried until it is successfully acknowledged.
   You can control its retry with time_to_live/max_retries to have a time/retry limit and
   control the retry frequency with retry_sleep_duration

.. tip:: To minimize the latency added by asynchronous notification, we 
   recommended placing the "log" pool on fast media.


Topic Management via CLI
------------------------

Fetch the configuration of all topics associated with tenants by running the
following command:

.. prompt:: bash #

   radosgw-admin topic list [--tenant={tenant}]  [--uid={user}]


Fetch the configuration of a specific topic by running the following command:

.. prompt:: bash #

   radosgw-admin topic get --topic={topic-name} [--tenant={tenant}]


Remove a topic by running the following command: 

.. prompt:: bash #

   radosgw-admin topic rm --topic={topic-name} [--tenant={tenant}]


Notification Performance Statistics
-----------------------------------

- ``pubsub_event_triggered``: a running counter of events that have at least one topic associated with them
- ``pubsub_event_lost``: a running counter of events that had topics associated with them, but that were not pushed to any of the endpoints
- ``pubsub_push_ok``: a running counter, for all notifications, of events successfully pushed to their endpoints
- ``pubsub_push_fail``: a running counter, for all notifications, of events that failed to be pushed to their endpoints
- ``pubsub_push_pending``: the gauge value of events pushed to an endpoint but not acked or nacked yet

.. note::

    ``pubsub_event_triggered`` and ``pubsub_event_lost`` are incremented per
    event on each notification, but ``pubsub_push_ok`` and ``pubsub_push_fail``
    are incremented per push action on each notification.

Bucket Notification REST API
----------------------------

Topics
~~~~~~

.. note::

    In all topic actions, the parameters are URL-encoded and sent in the
    message body using this content type:
    ``application/x-www-form-urlencoded``.
   

.. _Create a Topic:

Create a Topic
``````````````

This creates a new topic. Provide the topic with push endpoint parameters,
which will be used later when a notification is created. A response is
generated. A successful response includes the topic's `ARN
<https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>`_
(the "Amazon Resource Name", a unique identifier used to reference the topic).
To update a topic, use the same command that you used to create it (but when
updating, use the name of an existing topic and different endpoint values).

.. tip:: Any notification already associated with the topic must be re-created
   in order for the topic to update.

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
   [&Attributes.entry.9.key=persistent&Attributes.entry.9.value=true|false]
   [&Attributes.entry.10.key=cloudevents&Attributes.entry.10.value=true|false]
   [&Attributes.entry.11.key=mechanism&Attributes.entry.11.value=<mechanism>]
   [&Attributes.entry.12.key=time_to_live&Attributes.entry.12.value=<seconds to live>]
   [&Attributes.entry.13.key=max_retries&Attributes.entry.13.value=<retries number>]
   [&Attributes.entry.14.key=retry_sleep_duration&Attributes.entry.14.value=<sleep seconds>]
   [&Attributes.entry.15.key=Policy&Attributes.entry.15.value=<policy-JSON-string>]

Request parameters:

- push-endpoint: This is the URI of an endpoint to send push notifications to.
- OpaqueData: Opaque data is set in the topic configuration and added to all
  notifications that are triggered by the topic.
- persistent: This indicates whether notifications to this endpoint are
  persistent (=asynchronous) or not persistent. (This is "false" by default.)
- time_to_live: This will limit the time (in seconds) to retain the notifications.
  default value is taken from `rgw_topic_persistency_time_to_live`.
  providing a value overrides the global value.
  zero value means infinite time to live.
- max_retries: This will limit the max retries before expiring notifications.
  default value is taken from `rgw_topic_persistency_max_retries`.
  providing a value overrides the global value.
  zero value means infinite retries.
- retry_sleep_duration: This will control the frequency of retrying the notifications.
  default value is taken from `rgw_topic_persistency_sleep_duration`.
  providing a value overrides the global value.
  zero value mean there is no delay between retries.
- Policy: This will control who can access the topic in addition to the owner of the topic.
  The policy passed needs to be a JSON string similar to bucket policy.
  For example, one can send a policy string as follows::

    {
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": ["arn:aws:iam::usfolks:user/fred:subuser"]},
        "Action": ["sns:GetTopicAttributes","sns:Publish"],
        "Resource": ["arn:aws:sns:default::mytopic"],
      }]
    }

  Currently, we support only the following actions:
  - sns:GetTopicAttributes  To list or get existing topics
  - sns:SetTopicAttributes  To set attributes for the existing topic
  - sns:DeleteTopic         To delete the existing topic
  - sns:Publish             To be able to create/subscribe notification on existing topic

- HTTP endpoint

 - URI: ``http[s]://<fqdn>[:<port]``
 - port: This defaults to 80 for HTTP and 443 for HTTPS.
 - verify-ssl: This indicates whether the server certificate is validated by
   the client. (This is "true" by default.)
 - cloudevents: This indicates whether the HTTP header should contain
   attributes according to the `S3 CloudEvents Spec`_. (This is "false" by
   default.)

- AMQP0.9.1 endpoint

 - URI: ``amqp[s]://[<user>:<password>@]<fqdn>[:<port>][/<vhost>]``
 - user/password: This defaults to "guest/guest".
 - user/password: This must be provided only over HTTPS. Topic creation
   requests will otherwise be rejected.
 - port: This defaults to 5672 for unencrypted connections and 5671 for
   SSL-encrypted connections.
 - vhost: This defaults to "/".
 - verify-ssl: This indicates whether the server certificate is validated by
   the client. (This is "true" by default.)
 - If ``ca-location`` is provided and a secure connection is used, the
   specified CA will be used to authenticate the broker. The default CA will
   not be used.  
 - amqp-exchange: The exchanges must exist and must be able to route messages
   based on topics. This parameter is mandatory.
 - amqp-ack-level: No end2end acking is required. Messages may persist in the
   broker before being delivered to their final destinations. Three ack methods
   exist:

  - "none": The message is considered "delivered" if it is sent to the broker.
  - "broker": The message is considered "delivered" if it is acked by the broker (default).
  - "routable": The message is considered "delivered" if the broker can route to a consumer.

.. tip:: The topic-name (see :ref:`Create a Topic`) is used for the
   AMQP topic ("routing key" for a topic exchange).

- Kafka endpoint

 - URI: ``kafka://[<user>:<password>@]<fqdn>[:<port]``
 - ``use-ssl``: If this is set to "true", a secure connection is used to
   connect to the broker. (This is "false" by default.)
 - ``ca-location``: If this is provided and a secure connection is used, the
   specified CA will be used instead of the default CA to authenticate the
   broker. 
 - user/password: This should be provided over HTTPS. If not, the config parameter `rgw_allow_notification_secrets_in_cleartext` must be `true` in order to create topics.
 - user/password: This should be provided together with ``use-ssl``. If not, the broker credentials will be sent over insecure transport.
 - mechanism: may be provided together with user/password (default: ``PLAIN``). The supported SASL mechanisms are:

  - PLAIN
  - SCRAM-SHA-256
  - SCRAM-SHA-512
  - GSSAPI
  - OAUTHBEARER

 - port: This defaults to 9092.
 - kafka-ack-level: No end2end acking is required. Messages may persist in the
   broker before being delivered to their final destinations. Two ack methods
   exist:

  - "none": Messages are considered "delivered" if sent to the broker.
  - "broker": Messages are considered "delivered" if acked by the broker. (This
    is the default.)

.. note::

    - The key-value pair of a specific parameter need not reside in the same
      line as the parameter, and need not appear in any specific order, but it
      must use the same index.
    - Attribute indexing need not be sequential and need not start from any
      specific value.
    - `AWS Create Topic`_ provides a detailed explanation of the endpoint
      attributes format. In our case, however, different keys and values are
      used.

The response has the following format:

::

    <CreateTopicResponse xmlns="https://sns.amazonaws.com/doc/2010-03-31/">
        <CreateTopicResult>
            <TopicArn></TopicArn>
        </CreateTopicResult>
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </CreateTopicResponse>

The topic `ARN
<https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>`_
in the response has the following format:

::

   arn:aws:sns:<zone-group>:<tenant>:<topic>

Get Topic Attributes
````````````````````

This returns information about a specific topic. This includes push-endpoint
information, if provided.

::

   POST

   Action=GetTopicAttributes
   &TopicArn=<topic-arn>

The response has the following format:

::

    <GetTopicAttributesResponse>
        <GetTopicAttributesResult>
            <Attributes>
                <entry>
                    <key>User</key>
                    <value></value>
                </entry> 
                <entry>
                    <key>Name</key>
                    <value></value>
                </entry> 
                <entry>
                    <key>EndPoint</key>
                    <value></value>
                </entry> 
                <entry>
                    <key>TopicArn</key>
                    <value></value>
                </entry> 
                <entry>
                    <key>OpaqueData</key>
                    <value></value>
                </entry> 
            </Attributes>
        </GetTopicAttributesResult>
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </GetTopicAttributesResponse>

- User: the name of the user that created the topic.
- Name: the name of the topic.
- EndPoint: The JSON-formatted endpoint parameters, including:
   - EndpointAddress: The push-endpoint URL.
   - EndpointArgs: The push-endpoint args.
   - EndpointTopic: The topic name to be sent to the endpoint (can be different
     than the above topic name).
   - HasStoredSecret: This is "true" if the endpoint URL contains user/password 
     information. In this case, the request must be made over HTTPS. The "topic
     get" request will otherwise be rejected.
   - Persistent: This is "true" if the topic is persistent.
   - TimeToLive: This will limit the time (in seconds) to retain the notifications.
   - MaxRetries: This will limit the max retries before expiring notifications.
   - RetrySleepDuration: This will control the frequency of retrying the notifications.
- TopicArn: topic `ARN
  <https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>`_.
- OpaqueData: The opaque data set on the topic.
- Policy: Any access permission set on the topic.

Get Topic Information
`````````````````````

This returns information about a specific topic. This includes push-endpoint
information, if provided.  Note that this API is now deprecated in favor of the
AWS compliant `GetTopicAttributes` API.

::

   POST

   Action=GetTopic
   &TopicArn=<topic-arn>

The response has the following format:

::

    <GetTopicResponse>
        <GetTopicResult>
            <Topic>
                <User></User>
                <Name></Name>
                <EndPoint>
                    <EndpointAddress></EndpointAddress>
                    <EndpointArgs></EndpointArgs>
                    <EndpointTopic></EndpointTopic>
                    <HasStoredSecret></HasStoredSecret>
                    <Persistent></Persistent>
                </EndPoint>
                <TopicArn></TopicArn>
                <OpaqueData></OpaqueData>
            </Topic>
        </GetTopicResult>
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </GetTopicResponse>

- User: The name of the user that created the topic.
- Name: The name of the topic.
- EndpointAddress: The push-endpoint URL.
- EndpointArgs: The push-endpoint args.
- EndpointTopic: The topic name to be sent to the endpoint (which can be
  different than the above topic name).
- HasStoredSecret: This is "true" if the endpoint URL contains user/password
  information. In this case, the request must be made over HTTPS. The "topic
  get" request will otherwise be rejected.
- Persistent: "true" if topic is persistent.
- TopicArn: topic `ARN
  <https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>`_.
- OpaqueData: the opaque data set on the topic.
- Policy: Any access permission set on the topic.

Delete Topic
````````````

::

   POST

   Action=DeleteTopic
   &TopicArn=<topic-arn>

This deletes the specified topic.

.. note::

  - Deleting an unknown notification (for example, double delete) is not
    considered an error.
  - Deleting a topic does not automatically delete all notifications associated
    with it.

The response has the following format:

::

    <DeleteTopicResponse xmlns="https://sns.amazonaws.com/doc/2010-03-31/">
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </DeleteTopicResponse>

List Topics
```````````

List all topics associated with a tenant.

::

   POST

   Action=ListTopics

The response has the following format:

::

    <ListTopicsResponse xmlns="https://sns.amazonaws.com/doc/2010-03-31/">
        <ListTopicsResult>
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

- If the endpoint URL contains user/password information in any part of the
  topic, the request must be made over HTTPS. The "topic list" request will
  otherwise be rejected.

Set Topic Attributes
````````````````````

::

   POST

   Action=SetTopicAttributes
   &TopicArn=<topic-arn>&AttributeName=<attribute-name>&AttributeValue=<attribute-value>

This allows to set/modify existing attributes on the specified topic.

.. note::

  - The AttributeName passed will either be updated or created (if not exist) with AttributeValue passed.
  - Any unsupported AttributeName passed will result in error 400.

The response has the following format:

::

    <SetTopicAttributesResponse xmlns="https://sns.amazonaws.com/doc/2010-03-31/">
        <ResponseMetadata>
            <RequestId></RequestId>
        </ResponseMetadata>
    </SetTopicAttributesResponse>

Valid AttributeName that can be passed:

  - push-endpoint: This is the URI of an endpoint to send push notifications to.
  - OpaqueData: Opaque data is set in the topic configuration and added to all
    notifications that are triggered by the topic.
  - persistent: This indicates whether notifications to this endpoint are
    persistent (=asynchronous) or not persistent. (This is "false" by default.)
  - time_to_live: This will limit the time (in seconds) to retain the notifications.
  - max_retries: This will limit the max retries before expiring notifications.
  - retry_sleep_duration: This will control the frequency of retrying the notifications.
  - Policy: This will control who can access the topic other than owner of the topic.
  - verify-ssl: This indicates whether the server certificates must be validated by
    the client. This is "true" by default.
  - ``use-ssl``: If this is set to "true", a secure connection is used to
    connect to the broker. This is "false" by default.
  - cloudevents: This indicates whether the HTTP header should contain
    attributes according to the `S3 CloudEvents Spec`_. 
  - amqp-exchange: The exchanges must exist and must be able to route messages
    based on topics.
  - amqp-ack-level: No end2end acknowledgement is required. Messages may persist in the
    broker before being delivered to their final destinations. 
  - ``ca-location``: If this is provided and a secure connection is used, the
    specified CA will be used instead of the default CA to authenticate the
    broker. 
  - mechanism: may be provided together with user/password (default: ``PLAIN``).
  - kafka-ack-level: No end2end acknowledgement is required. Messages may persist in the
    broker before being delivered to their final destinations. 

Notifications
~~~~~~~~~~~~~

Detailed under: `Bucket Operations`_.

.. note::

    - "Abort Multipart Upload" request does not emit a notification
    - Both "Initiate Multipart Upload" and "POST Object" requests will emit an ``s3:ObjectCreated:Post`` notification

Events
~~~~~~

Events are in JSON format (regardless of the actual endpoint), and are S3-compatible.
For example:

::

   {"Records":[
       {
           "eventVersion":"2.1",
           "eventSource":"ceph:s3",
           "awsRegion":"zonegroup1",
           "eventTime":"2019-11-22T13:47:35.124724Z",
           "eventName":"ObjectCreated:Put",
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
                   "arn":"arn:aws:s3:zonegroup1::mybucket1",
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

- awsRegion: The zonegroup.
- eventTime: The timestamp, indicating when the event was triggered.
- eventName: For the list of supported events see: `S3 Notification
  Compatibility`_. Note that eventName values do not start with the `s3:`
  prefix.
- userIdentity.principalId: The user that triggered the change.
- requestParameters.sourceIPAddress: not supported
- responseElements.x-amz-request-id: The request ID of the original change.
- responseElements.x_amz_id_2: The RGW on which the change was made.
- s3.configurationId: The notification ID that created the event.
- s3.bucket.name: The name of the bucket.
- s3.bucket.ownerIdentity.principalId: The owner of the bucket.
- s3.bucket.arn: The ARN of the bucket.
- s3.bucket.id: The ID of the bucket. (This is an extension to the S3
  notification API.)
- s3.object.key: The object key.
- s3.object.size: The object size.
- s3.object.eTag: The object etag.
- s3.object.versionId: The object version, if the bucket is versioned. When a
  copy is made, it includes the version of the target object. When a delete
  marker is created, it includes the version of the delete marker.
- s3.object.sequencer: The monotonically-increasing identifier of the "change
  per object" (hexadecimal format).
- s3.object.metadata: Any metadata set on the object that is sent as
  ``x-amz-meta-`` (that is, any metadata set on the object that is sent as an
  extension to the S3 notification API).
- s3.object.tags: Any tags set on the object. (This is an extension to the S3
  notification API.)
- s3.eventId: The unique ID of the event, which could be used for acking. (This
  is an extension to the S3 notification API.)
- s3.opaqueData: This means that "opaque data" is set in the topic configuration
  and is added to all notifications triggered by the topic. (This is an
  extension to the S3 notification API.)

.. _S3 Notification Compatibility: ../s3-notification-compatibility
.. _AWS Create Topic: https://docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html
.. _Bucket Operations: ../s3/bucketops
.. _S3 CloudEvents Spec: https://github.com/cloudevents/spec/blob/main/cloudevents/adapters/aws-s3.md
