.. _jaegertracing:

===================
Distributed tracing
===================

Ceph daemons can export traces in the OpenTelemetry format to a Jaeger
backend, where each trace shows what one request did and where its time
went. Ceph offers two kinds of traces:

* **Slow-op traces** (OSD): only operations slower than a threshold are
  traced. The trace is built after the operation completes, from the event
  timeline that the op tracker records for every operation anyway, so
  operations faster than the threshold cost nothing extra. This is the kind to
  leave on in production.
* **Tracing of every request** (OSD and RGW): every request is traced as it
  runs. This shows the complete request flow between RGW and the OSDs, but it
  adds measurable CPU and latency to every operation, so enable it only while
  investigating.

Terminology
===========

* **Trace**: the path of one request through the system.
* **Span**: one timed unit of work in a trace, with a name, a start and end
  time, attributes, and timestamped events. Spans nest: a slow-op trace has
  one span for the operation and one child span for each phase of it.
* **Jaeger**: the tracing backend that receives, stores and displays traces.
  Its web UI listens on port 16686.

Quick start
===========

A single Jaeger v2 container is enough: every Ceph daemon can send spans to
it directly, and it needs no agents on other hosts and no separate database.

#. Start Jaeger on a host that the OSDs can reach:

   .. prompt:: bash $

      podman run -d --name jaeger --network host jaegertracing/jaeger:latest

   Jaeger v2 accepts spans over OTLP/HTTP on port 4318 and serves its UI on
   port 16686. By default it keeps traces in memory, so they are lost when
   the container restarts.

#. Point the OSDs at it. This takes effect without restarting the OSDs:

   .. prompt:: bash $

      ceph config set osd trace_exporter otlp
      ceph config set osd trace_otlp_endpoint http://<jaeger host>:4318/v1/traces

#. Trace operations slower than half a second:

   .. prompt:: bash $

      ceph config set osd osd_op_trace_slow_threshold 0.5

#. Open ``http://<jaeger host>:16686`` and select the ``osd`` service.

.. _slow-op-traces:

Slow-op traces
==============

Every operation that takes at least ``osd_op_trace_slow_threshold`` seconds
is exported as a trace when it completes. The default of ``0`` disables slow-op
traces. They are independent of ``jaeger_tracing_enable``, which may stay
``false``.

.. confval:: osd_op_trace_slow_threshold
.. confval:: osd_op_trace_max_per_sec

What a slow-op trace contains
-----------------------------

* **One span for the operation**, named after its message type (for example
  ``osd_op`` for a client request, ``osd_repop`` for a replicated write on a
  replica). It starts when the operation arrived and ends when it completed.
* **Every op tracker event** as a span event, with its original timestamp.
  These are the same events that ``ceph tell osd.N dump_historic_ops`` shows,
  such as ``queued_for_pg``, ``reached_pg``, ``waiting for rw locks``,
  ``started`` and ``commit_sent``.
* **A child span for each phase** that took at least 5% of the operation,
  named after what the operation was doing: ``receive``, ``queued for PG``,
  the reason it was delayed (for example ``waiting for rw locks``),
  ``execute``, ``local commit`` and ``reply``. While a write waits for its
  replicas, each replica gets its own phase, ``replica osd.N``, all starting
  when the sub-operations were sent, so the slowest replica is the longest
  bar. For example, in a replicated write where one replica was slow::

    osd_op                    649 ms
      queued for PG            41 ms
      replica osd.1           170 ms
      replica osd.2           570 ms

  Events the phase names do not cover keep names made of the two events
  around them, as ``<event> -> <next event>``. An operation that spent at
  least 90% of its time in one phase gets no child spans; its span and
  events already say where the time went.
* **Attributes** that Jaeger can search on, for example ``pool_name=rbd``
  or ``role=replica``:

  - ``description``: the operation, as in the op tracker
  - ``osd``, ``source`` (the sender), ``reqid`` and ``duration_s``
  - ``role``: ``primary`` or ``replica``, the part this OSD played in the
    request. A reply from a replica is handled by the primary.
  - ``pool``, ``pool_name``, ``pg`` and ``osdmap_epoch`` (the epoch the
    sender used)
  - ``object``, and ``namespace`` if it is not the default one
  - for client requests: ``op_type`` (``read``, ``write`` or
    ``read-write``), ``ops`` (for example ``writefull,setxattr``) and
    ``bytes`` (written, or requested for reads)
  - ``msg_bytes``: the size of the message as received

One trace per request
---------------------

A replicated write shows up as an ``osd_op`` on the primary and an
``osd_repop`` on each replica. They land in one trace: the replicas'
sub-operations hang off the primary's ``osd_op``, which puts the primary's
wait for ``osd.2`` next to what ``osd.2`` was doing with the sub-operation at
the time. Every OSD derives the IDs involved from the request ID
(``reqid``) and the cluster fsid, so this needs no extra data between OSDs.
The primary does not export the replicas' acknowledgements
(``osd_repop_reply``) as spans of their own: the ``replica osd.N`` phases of
its ``osd_op`` already show when each one arrived.
A request that no client traced hangs off a root span that no OSD exports,
so Jaeger may warn about a missing parent span; that is expected.

Only slow operations are traced, and each OSD applies its own rate limit, so
a trace holds the operations that crossed ``osd_op_trace_slow_threshold`` on
their own OSD, not necessarily all of them. If a replica's sub-operation was
slow but the primary's operation was not, the sub-operation's parent is
missing from the trace.

.. _slow-request-traces:

End to end: RGW and the OSDs
----------------------------

RGW can trace slow S3 and Swift requests in the same way, and the OSDs then
place their slow operations under the request::

  rgw  put_obj                     712 ms
    osd.0  osd_op                  649 ms
      queued for PG                 41 ms
      replica osd.1                170 ms
      replica osd.2                570 ms
      osd.2  osd_repop             560 ms
        ...

.. confval:: rgw_trace_slow_threshold
.. confval:: rgw_trace_max_per_sec
.. confval:: osd_op_trace_slow_require_context

With ``rgw_trace_slow_threshold`` set, every request that is not traced live
carries a trace context to the OSDs: a trace ID and the ID of the request's
span, but no span. RGW exports the request's span only if the request took
at least ``rgw_trace_slow_threshold``, after it completes, and the OSDs export
their slow operations under it. The primary OSD forwards the context to the
replicas with each sub-operation, so their sub-operations join the same
trace. Set ``osd_op_trace_slow_threshold`` to the same value, so that the OSD
side of a slow request is traced too. With a lower value, the OSDs also trace
slow operations of requests that were not slow enough for RGW, and those
reach the tracing backend without a request span above them.

The cost for requests that are not slow is 24 random bytes per request in
RGW, and about 25 bytes more per message from RGW to the primary and from
the primary to each replica. The context is marked as not sampled, so no OSD
creates live spans because of it.

Requests traced live (``jaeger_tracing_enable``) pass their own, sampled
context instead, and the OSDs' slow operations join those traces the same
way. RGW passes a context with the data writes of an object and with the
bucket index updates around them, so a PUT that was slow because of its
bucket index shows that too. Operations that reach the OSDs without a
context form a trace of their own, as above.

Much of what RGW sends to the OSDs is its own background work: locks,
watches and log trimming on the zone's log and control pools (for example
``default.rgw.log`` and ``default.rgw.control``).
These operations carry no context, and on a busy gateway they make up most
of the slow operations the OSDs trace, and use up their rate limit. With
``osd_op_trace_slow_require_context`` set, the OSDs trace only operations
that arrive with a context. Clients that pass none, such as RBD and CephFS,
are then not traced either, so set it only on clusters where RGW is the
client that matters.

To trace slow S3 requests end to end:

.. prompt:: bash $

   ceph config set global rgw_trace_slow_threshold 1
   ceph config set osd osd_op_trace_slow_threshold 1
   ceph config set osd osd_op_trace_slow_require_context true

On a small test cluster, S3 PUT latency with these settings did not differ
from latency with tracing off: over eight rounds each, the median and the
99th percentile were within the variation from one round to the next.

Finding the trace of a slow operation
-------------------------------------

A traced operation shows its ``trace_id`` in the op history. The slowest
recent operations are listed first by:

.. prompt:: bash $

   ceph tell osd.0 dump_historic_ops_by_duration

Search for that ID in the Jaeger UI to open the trace.

An operation that never completes is never traced that way. So when the OSD
reports an operation as a slow request (older than ``osd_op_complaint_time``,
30 seconds by default) and slow-op tracing is on, it also exports what the
operation did so far, once, as a span with ``in_flight`` set that ends with
the phase the operation is still in, such as ``waiting for rw locks -> (in
flight)``. ``ceph tell osd.N dump_ops_in_flight`` then shows the operation's
``trace_id``, and so do the per-operation slow request lines in the cluster
log, which the OSD writes when ``osd_aggregated_slow_ops_logging`` is off. If
the operation completes later, its full span joins the same trace. At most
``osd_op_trace_max_per_sec`` such operations are traced per health check. The history keeps
``osd_op_history_size`` operations from the last ``osd_op_history_duration``
seconds; ``dump_historic_slow_ops`` keeps only operations slower than
``osd_op_history_slow_op_threshold`` (10 seconds by default).

Rate limit
----------

When a cluster has trouble, many operations become slow at once. Each OSD
exports at most ``osd_op_trace_max_per_sec`` slow-op traces per second
(default 10) and skips the rest. Operations that are not traced for another
reason, such as ``osd_op_trace_slow_require_context``, do not count
against the limit. The ``trackedop`` perf counters count both:

.. prompt:: bash $

   ceph tell osd.0 perf dump trackedop

* ``slow_op_traces``: operations exported as traces
* ``slow_op_traces_dropped``: slow operations skipped because of the rate limit

RGW limits its request traces with ``rgw_trace_max_per_sec`` (default 100)
and counts them in ``slow_request_traces`` and ``slow_request_traces_dropped``
of its ``rgw`` perf counters. A request's trace is one span in RGW, while its
operations are traced by every OSD they reach, each within its own limit. If
RGW drops requests while the OSDs still trace their operations, those
operations appear without the request above them; raise
``rgw_trace_max_per_sec`` until ``slow_request_traces_dropped`` stays at 0.
To keep or drop whole traces, send the spans through an OpenTelemetry
Collector with the ``tail_sampling`` processor, which decides per trace ID
once all of a trace's spans have arrived.

Cost
----

The decision to trace, and the building of the trace, happen after the
operation completed, on the op tracker's history thread rather than in the
I/O path. An operation faster than the threshold costs nothing beyond what the
op tracker already does. Slow-op traces therefore need the op tracker
(``osd_enable_op_tracker``, enabled by default).

If no traces appear
-------------------

#. Check that ``osd_op_trace_slow_threshold`` is set and that some operations
   are slower than it: ``ceph tell osd.N dump_historic_ops_by_duration``.
#. Check the counters above. If ``slow_op_traces`` rises, the OSD exports
   traces; if ``slow_op_traces_dropped`` rises, raise
   ``osd_op_trace_max_per_sec``.
#. Check where the OSD sends them. The OSD log records every change of
   destination at debug level 1 of the ``trace`` subsystem, for example
   ``otel_tracing: exporting spans over OTLP/HTTP to
   http://10.0.0.5:4318/v1/traces``.
#. Check that the OSD hosts can reach that address: TCP port 4318 for OTLP.
   With the ``jaeger`` exporter, spans travel over UDP, so a firewall drops
   them without any error; allow UDP port 6831 (or 6799 for a cephadm agent).

Where spans are sent
====================

.. confval:: trace_exporter
.. confval:: trace_otlp_endpoint
.. confval:: jaeger_agent_host
.. confval:: jaeger_agent_port

Ceph can export spans with either of two protocols:

* ``otlp`` posts them over HTTP to ``trace_otlp_endpoint``. OTLP is the
  OpenTelemetry protocol, so the same setting works with Jaeger v2, Grafana
  Tempo, the OpenTelemetry Collector and most commercial tracing services.
  Delivery is acknowledged, so spans are not silently lost on the network.
  This is the recommended exporter.
* ``jaeger`` (the default, for compatibility) sends them over UDP to
  ``jaeger_agent_host`` and ``jaeger_agent_port``. The defaults, ``localhost``
  and port 6799, match a Jaeger agent on every host as deployed by cephadm
  (see below). Jaeger v2 still accepts this protocol on UDP port 6831, but it
  is Jaeger-specific and deprecated by OpenTelemetry.

OSDs apply a change to any of these options without restarting; RGW reads them
when it starts. Export happens on a background thread, never in the I/O path.

.. _jaegertracing-enable:

Tracing every request
=====================

Tracing of every request is disabled by default. It can be enabled for all
daemons or for one daemon type at a time:

.. prompt:: bash $

   ceph config set global jaeger_tracing_enable true
   ceph config set <entity> jaeger_tracing_enable true

.. warning::

   Every request then creates spans in the I/O path. On an OSD, expect a
   noticeable rise in CPU per operation and in latency. For operations that
   matter most, prefer :ref:`slow-op traces <slow-op-traces>`.

Traces in RGW
-------------

Traces from RGW are listed under the ``rgw`` service in the Jaeger UI.

Every user request is traced. Each trace carries the ``Operation name``,
``User id``, ``Object name`` and ``Bucket name`` tags, and multipart uploads
also carry ``Upload id``. Request traces are named ``<command> <transaction
id>``.

A multipart upload also gets a trace of its own, with a span for each request
that belongs to the upload, including every ``Put Object`` request. These
traces are named ``multipart_upload <upload id>``.

rgw service in the Jaeger UI:

.. image:: ./rgw_jaeger.png
  :width: 400

osd service in the Jaeger UI:

.. image:: ./osd_jaeger.png
  :width: 400

Deploying Jaeger with cephadm
=============================

cephadm can deploy Jaeger as a set of services, with a Jaeger agent on every
host. See :ref:`Cephadm Jaeger services deployment <cephadm-tracing>`.

Further reading: `Jaeger documentation <https://www.jaegertracing.io/docs/>`_.
