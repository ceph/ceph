=========================
PubSub Sync Module
=========================

.. versionadded:: Nautilus

This sync module provides a publish and subscribe mechanism for the object store modification
events. Events are published into defined topics. Topics can be subscribed to, and events
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

A new REST api has been defined to provide configuration and control interfaces for the pubsub
mechanisms.

Events are stored as rgw objects in a special bucket, under a special user. Events cannot
be accessed directly, but need to be pulled and acked using the new REST api.



PubSub Tier Type Configuration
-------------------------------------

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

How to Configure
~~~~~~~~~~~~~~~~

See `Multisite Configuration`_ for how to multisite config instructions. The pubsub sync module requires a creation of a new zone. The zone
tier type needs to be defined as ``pubsub``:

::

    # radosgw-admin zone create --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --endpoints={http://fqdn}[,{http://fqdn}]
                                --tier-type=pubsub


The tier configuration can be then done using the following command

::

    # radosgw-admin zone modify --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --tier-config={key}={val}[,{key}={val}]

The ``key`` in the configuration specifies the config variable that needs to be updated, and
the ``val`` specifies its new value. Nested values can be accessed using period. For example:

::

    # radosgw-admin zone modify --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --tier-config=uid=pubsub


A configuration field can be removed by using ``--tier-config-rm={key}``.

PubSub Performance Stats
-------------------------
- **pubsub_event_triggered**: running counter of events with at lease one pubsub topic associated with them
- **pubsub_event_lost**: running counter of events that had  pubsub topics and subscriptions associated with them but that were not stored or pushed to any of the subscriptions
- **pubsub_store_ok**: running counter, for all subscriptions, of stored pubsub events 
- **pubsub_store_fail**: running counter, for all subscriptions, of pubsub events that needed to be stored but failed
- **pubsub_push_ok**: running counter, for all subscriptions, of pubsub events successfully pushed to their endpoint
- **pubsub_push_fail**: running counter, for all subscriptions, of pubsub events failed to be pushed to their endpoint
- **pubsub_push_pending**: gauge value of pubsub events pushed to a endpoined but not acked or nacked yet

Note that **pubsub_event_triggered** and **pubsub_event_lost** are incremented per event, while: **pubsub_store_ok**, **pubsub_store_fail**, **pubsub_push_ok**, **pubsub_push_fail**, are incremented per store/push action on each subscriptions.

PubSub REST API
-------------------------


Topics
~~~~~~

Create a Topic
``````````````````````````

This will create a new topic.

::

   PUT /topics/<topic-name>


Get Topic Information
````````````````````````````````

Returns information about specific topic. This includes subscriptions to that topic.

::

   GET /topics/<topic-name>



Delete Topic
````````````````````````````````````

::

   DELETE /topics/<topic-name>

Delete the specified topic.

List Topics
````````````````````````````````````

List all topics that user defined.

::

   GET /topics



Notifications
~~~~~~~~~~~~~

Create a Notification
``````````````````````````

This will create a publisher for a specific bucket into a topic.

::

   PUT /notifications/bucket/<bucket>?topic=<topic-name>[&events=<event>[,<event>]]


Request Params:
 - topic-name: name of topic
 - event: event type (string), one of: OBJECT_CREATE, OBJECT_DELETE 



Delete Notification Information
````````````````````````````````

Delete publisher from a specific bucket into a specific topic.

::

   DELETE /notifications/bucket/<bucket>?topic=<topic-name>

Request Params:
 - topic-name: name of topic



Create Subscription
````````````````````````````````````

Creates a new subscription.

::

   PUT /subscriptions/<sub-name>?topic=<topic-name>[&push-endpoint=<endpoint>[&amqp-exchange=<exchange>][&amqp-ack-level=<level>][&verify-ssl=true|false]]

Request Params:

 - topic-name: name of topic
 - push-endpoint: URI of endpoint to send push notification to

  - URI schema is: ``http|amqp://[<user>:<password>@]<fqdn>[:<port>][/<amqp-vhost>]``
  - Same schema is used for HTTP and AMQP endpoints (except amqp-vhost which is specific to AMQP)
  - Default values for HTTP: no user/password, port 80
  - Default values for AMQP: user/password=guest/guest, port 5672, amqp-vhost is "/"

 - verify-ssl: can be used with https endpoints (ignored for other endpoints), indicate whether the server certificate is validated or not ("true" by default)
 - amqp-exchange: mandatory parameter for AMQP endpoint. The exchanges must exist and be able to route messages based on topics
 - amqp-ack-level: 2 ack levels exist: "none" - message is considered "delivered" if sent to broker; 
   "broker" message is considered "delivered" if acked by broker. 
   No end2end acking is required, as messages may persist in the broker before delivered into their final destination

Get Subscription Info
````````````````````````````````````

Returns info about specific subscription

::

   GET /subscriptions/<sub-name>


Delete Subscription
`````````````````````````````````

Removes a subscription

::

   DELETE /subscriptions/<sub-name>


Events
~~~~~~

Pull Events
`````````````````````````````````

Pull events sent to a specific subscription

::

   GET /subscriptions/<sub-name>?events[&max-entries=<max-entries>][&marker=<marker>]

Request Params:
 - marker: pagination marker for list of events, if not specified will start from the oldest
 - max-entries: max number of events to return


Ack Event
`````````````````````````````````

Ack event so that it can be removed from the subscription history.

::

   POST /subscriptions/<sub-name>?ack&event-id=<event-id>


Request Params:
 - event-id: id of event to be acked

.. _Multisite Configuration: ./multisite.rst
