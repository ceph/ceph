JAEGER- DISTRIBUTED TRACING
===========================

Jaeger  provides ready to use tracing services for distributed
systems and is becoming the widely used standard because of their simplicity and
standardization.


BASIC ARCHITECTURE AND TERMINOLOGY
----------------------------------

* TRACE: A trace shows the data/execution path through a system.
* SPAN: A single unit of a trace. A data structure that stores information such
  as the operation name, timestamps, and the ordering within a trace.
* JAEGER CLIENT: Language-specific implementations of the OpenTracing API.
* JAEGER AGENT: A daemon that listens for spans sent over User Datagram
  Protocol. The agent is meant to be placed on the same host as the
  instrumented application. (The Jaeger agent acts like a sidecar listener.)
* JAEGER COLLECTOR: A daemon that receives spans sent by the Jaeger agent. The
  Jaeger collector then stitches the spans together to form a trace. (A database
  can be enabled to persist these traces).
* JAEGER QUERY AND CONSOLE FRONTEND: The UI-based frontend that presents
  reports of the jaeger traces. Accessible at  http://<jaeger frontend host>:16686.


read more about jaeger tracing:.

  https://www.jaegertracing.io/docs/


JAEGER DEPLOYMENT
-----------------

there are couple of ways to deploy jaeger.
please refer to:

`jaeger deployment <https://www.jaegertracing.io/docs/1.25/deployment/>`_

`jaeger performance tuning <https://www.jaegertracing.io/docs/1.25/performance-tuning/>`_


In addition, spans are being sent to local jaeger agent, so the jaeger agent must be running on each host (not in all-in-one mode).
otherwise, spans of hosts without active jaeger agent will be lost.

HOW TO ENABLE TRACING IN CEPH
-----------------------------

tracing in Ceph is disabled by default.
it could be enabled globally, or for each entity separately (e.g. rgw).

  Enable tracing globally::

      $ ceph config set global jaeger_tracing_enable true


  Enable tracing for each entity::

      $ ceph config set <entity> jaeger_tracing_enable true


TRACES IN RGW
-------------

traces of RGW can be found under Service `rgw` in Jaeger Frontend.

REQUESTS
^^^^^^^^
every user request is being traced. each trace contains tags for
`Operation name`, `User id`, `Object name` and `Bucket name`.

there is also `Upload id` tag for Multipart upload operations.

request trace is named `<command> <transaction id>`.

MULTIPART UPLOAD
^^^^^^^^^^^^^^^^
there is a trace, that consists a span for each request that made by that multipart upload, including all `Put Object` requests.

multipart trace is named `multipart_upload <upload id>`.


rgw service in Jaeger Frontend:

.. image:: ./rgw_jaeger.png
  :width: 400


osd service in Jaeger Frontend:

.. image:: ./osd_jaeger.png
  :width: 400
