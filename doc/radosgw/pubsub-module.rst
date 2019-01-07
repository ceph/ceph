=========================
PubSub Sync Module
=========================

.. versionadded:: Nautilus

This sync module provides a publish and subscribe mechanism for the object store modification
events. Events are published into defined topics. Topics can be subscribed to, and events
can be pulled from them. Events need to be acked. Also, events will expire and disappear
after a period of time. A basic push notification mechanism exists too, but it is not
reliable.

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

* ``endpoint`` (string)

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

PubSub REST API
-------------------------

.. versionadded:: Luminous


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

   PUT /notifications/<bucket>?topic=<topic-name>[&events=<event>[,<event>]]


Request Params:
 - topic-name: name of topic
 - event: event type (string)



Delete Notification Information
````````````````````````````````

Delete publisher from a specific bucket into a specific topic.

::

   DELETE /notifications/<bucket>?topic=<topic-name>

Request Params:
 - topic-name: name of topic



Create Subscription
````````````````````````````````````

Creates a new subscription.

::

   PUT /subscriptions/<sub-name>?topic=<topic-name>[&push-endpoint=<endpoint>]

Request Params:
 - topic-name: name of topic
 - push-endpoint: url of endpoint to send push notification to



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

.. _Multisite Configuration: ./multisite
