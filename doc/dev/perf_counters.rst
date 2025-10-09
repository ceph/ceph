.. _Perf Counters:

===============
 Perf counters
===============

The perf counters provide generic internal infrastructure for gauges and counters.  The counted values can be both integer and float. There is also an "average" type (normally float) that combines a sum and num counter which can be divided to provide an average.

The intention is that this data will be collected and aggregated by a tool like ``collectd`` or ``statsd`` and fed into a tool like ``graphite`` for graphing and analysis.  Also, note the :doc:`../mgr/prometheus` and the :doc:`../mgr/telemetry`.

Users and developers can also access perf counter data locally to check a cluster's overall health, identify workload patterns, monitor cluster performance by daemon types, and troubleshoot issues with latency, throttling, memory management, etc. (see :ref:`Access`)

.. _Access:

Access
------

The perf counter data is accessed via the admin socket.  For example::

   ceph daemon osd.0 perf schema
   ceph daemon osd.0 perf dump


Collections
-----------

The values are grouped into named collections, normally representing a subsystem or an instance of a subsystem.  For example, the internal ``throttle`` mechanism reports statistics on how it is throttling, and each instance is named something like::


    throttle-msgr_dispatch_throttler-hbserver
    throttle-msgr_dispatch_throttler-client
    throttle-filestore_bytes
    ...


Schema
------

The ``perf schema`` command dumps a json description of which values are available, and what their type is.  Each named value as a ``type`` bitfield, with the following bits defined.

+------+-------------------------------------+
| bit  | meaning                             |
+======+=====================================+
| 1    | floating point value                |
+------+-------------------------------------+
| 2    | unsigned 64-bit integer value       |
+------+-------------------------------------+
| 4    | average (sum + count pair), where   |
+------+-------------------------------------+
| 8    | counter (vs gauge)                  |
+------+-------------------------------------+

Every value will have either bit 1 or 2 set to indicate the type
(float or integer).

If bit 8 is set (counter), the value is monotonically increasing and
the reader may want to subtract off the previously read value to get
the delta during the previous interval.

If bit 4 is set (average), there will be two values to read, a sum and
a count.  If it is a counter, the average for the previous interval
would be sum delta (since the previous read) divided by the count
delta.  Alternatively, dividing the values outright would provide the
lifetime average value.  Normally these are used to measure latencies
(number of requests and a sum of request latencies), and the average
for the previous interval is what is interesting.

Instead of interpreting the bit fields, the ``metric type`` has a
value of either ``gauge`` or ``counter``, and the ``value type``
property will be one of ``real``, ``integer``, ``real-integer-pair``
(for a sum + real count pair), or ``integer-integer-pair`` (for a
sum + integer count pair).

Here is an example of the schema output::

  {
    "throttle-bluestore_throttle_bytes": {
        "val": {
            "type": 2,
            "metric_type": "gauge",
            "value_type": "integer",
            "description": "Currently available throttle",
            "nick": ""
        },
        "max": {
            "type": 2,
            "metric_type": "gauge",
            "value_type": "integer",
            "description": "Max value for throttle",
            "nick": ""
        },
        "get_started": {
            "type": 10,
            "metric_type": "counter",
            "value_type": "integer",
            "description": "Number of get calls, increased before wait",
            "nick": ""
        },
        "get": {
            "type": 10,
            "metric_type": "counter",
            "value_type": "integer",
            "description": "Gets",
            "nick": ""
        },
        "get_sum": {
            "type": 10,
            "metric_type": "counter",
            "value_type": "integer",
            "description": "Got data",
            "nick": ""
        },
        "get_or_fail_fail": {
            "type": 10,
            "metric_type": "counter",
            "value_type": "integer",
            "description": "Get blocked during get_or_fail",
            "nick": ""
        },
        "get_or_fail_success": {
            "type": 10,
            "metric_type": "counter",
            "value_type": "integer",
            "description": "Successful get during get_or_fail",
            "nick": ""
        },
        "take": {
            "type": 10,
            "metric_type": "counter",
            "value_type": "integer",
            "description": "Takes",
            "nick": ""
        },
        "take_sum": {
            "type": 10,
            "metric_type": "counter",
            "value_type": "integer",
            "description": "Taken data",
            "nick": ""
        },
        "put": {
            "type": 10,
            "metric_type": "counter",
            "value_type": "integer",
            "description": "Puts",
            "nick": ""
        },
        "put_sum": {
            "type": 10,
            "metric_type": "counter",
            "value_type": "integer",
            "description": "Put data",
            "nick": ""
        },
        "wait": {
            "type": 5,
            "metric_type": "gauge",
            "value_type": "real-integer-pair",
            "description": "Waiting latency",
            "nick": ""
        }
  }


Dump
----

The actual dump is similar to the schema, except that average values are grouped.  For example::

 {
   "throttle-msgr_dispatch_throttler-hbserver" : {
      "get_or_fail_fail" : 0,
      "get_sum" : 0,
      "max" : 104857600,
      "put" : 0,
      "val" : 0,
      "take" : 0,
      "get_or_fail_success" : 0,
      "wait" : {
         "avgcount" : 0,
         "sum" : 0
      },
      "get" : 0,
      "take_sum" : 0,
      "put_sum" : 0
   },
   "throttle-msgr_dispatch_throttler-client" : {
      "get_or_fail_fail" : 0,
      "get_sum" : 82760,
      "max" : 104857600,
      "put" : 2637,
      "val" : 0,
      "take" : 0,
      "get_or_fail_success" : 0,
      "wait" : {
         "avgcount" : 0,
         "sum" : 0
      },
      "get" : 2637,
      "take_sum" : 0,
      "put_sum" : 82760
   }
 }

.. _Labeled Perf Counters:

Labeled Perf Counters
---------------------

.. note:: Labeled perf counters were introduced in the Reef release of Ceph.

A Ceph daemon has the ability to emit a set of perf counter instances with varying labels. These counters are intended for visualizing specific metrics in 3rd party tools like Prometheus and Grafana.

For example, the below counters show the number of put requests for different users on different buckets::

  {
      "rgw": [
          {
              "labels": {
                  "Bucket: "bkt1",
                  "User: "user1",
              },
              "counters": {
                  "put": 1,
              },
          },
          {
              "labels": {},
              "counters": {
                  "put": 4,
              },
          },
          {
              "labels": {
                  "Bucket: "bkt1",
                  "User: "user2",
              },
              "counters": {
                  "put": 3,
              },
          },
      ]
  }

All labeled and unlabeled perf counters can be viewed with ``ceph daemon {daemon id} counter dump``. 

All labeled and unlabeled perf counter's schema can be viewed with ``ceph daemon {daemon id} counter schema``.

In the above example the second counter without labels is a counter that would also be shown in ``ceph daemon {daemon id} perf dump``.

Since the ``counter dump`` and ``counter schema`` commands can be used to view both types of counters it is not recommended to use the ``perf dump`` and ``perf schema`` commands which are retained for backwards compatibility and continue to emit only non-labeled counters.

Some perf counters that are emitted via ``perf dump`` and ``perf schema`` may become labeled in future releases and as such will no longer be emitted by ``perf dump`` and ``perf schema`` respectively.
