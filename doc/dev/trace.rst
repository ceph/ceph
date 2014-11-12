==================
 Tracing Ceph
==================

Ceph can use Blkin, a library created by Marios Kogias and others,
which enables tracking a specific request from the time it enters
the system at higher levels till it is finally served by RADOS.

In general, Blkin implements the tracing semantics described in the Dapper
paper http://static.googleusercontent.com/media/research.google.com/el//pubs/archive/36356.pdf
in order to trace the causal relationships between the different
processing phases that an IO request may trigger. The goal is an end-to-end
visualisation of the request's route in the system, accompanied by information
concerning latencies in each processing phase. Thanks to LTTng this can happen
with a minimal overhead and in realtime. In order to visualize the results we
have integrated Twitter's Zipkin http://twitter.github.io/zipkin/
(which is a tracing system entirely based on Dapper) with LTTng.

Testing Blkin
=============

It's easy to test Ceph's Blkin tracing. Compile Ceph with the Blkin
changes, then launch Ceph with the vstart script so you can see the
possible tracepoints.::

  cd src
  OSD=3 MON=3 RGW=1 ./vstart.sh -n
  lttng list --userspace

You'll see something like the following:::

  UST events:
  -------------
   
  PID: 8987 - Name: ./ceph-osd
        zipkin:timestamp (loglevel: TRACE_WARNING (4)) (type: tracepoint)
        zipkin:keyval (loglevel: TRACE_WARNING (4)) (type: tracepoint)
        ust_baddr_statedump:soinfo (loglevel: TRACE_DEBUG_LINE (13)) (type: tracepoint)
   
  PID: 8407 - Name: ./ceph-mon
        zipkin:timestamp (loglevel: TRACE_WARNING (4)) (type: tracepoint)
        zipkin:keyval (loglevel: TRACE_WARNING (4)) (type: tracepoint)
        ust_baddr_statedump:soinfo (loglevel: TRACE_DEBUG_LINE (13)) (type: tracepoint)
   
  ...

Next, stop Ceph so that the tracepoints can be enabled.::

  ./stop.sh

Start up an LTTng session and enable the tracepoints.::

  lttng create blkin-test
  lttng enable-event --userspace zipkin:timestamp
  lttng enable-event --userspace zipkin:keyval
  lttng start

Then start up Ceph again.::

  OSD=3 MON=3 RGW=1 ./vstart.sh -n

You may want to check that ceph is up.::

  ./ceph status

Now put something in usin rados, check that it made it, get it back, and remove it.::

  rados put test-object-1 ./vstart.sh --pool=data
  rados -p data ls
  ceph osd map data test-object-1
  rados get test-object-1 ./vstart-copy.sh --pool=data
  md5sum vstart*
  rados rm test-object-1 --pool=data

Then stop the LTTng session and see what was collected.::

  lttng stop
  lttng view

You'll see something like:::

  [13:09:07.755054973] (+?.?????????) scruffy zipkin:timestamp: { cpu_id = 5 }, { trace_name = "Main", service_name = "MOSDOp", port_no = 0, ip = "0.0.0.0", trace_id = 7492589359882233221, span_id = 2694140257089376129, parent_span_id = 0, event = "Message allocated" }
  [13:09:07.755071569] (+0.000016596) scruffy zipkin:keyval: { cpu_id = 5 }, { trace_name = "Main", service_name = "MOSDOp", port_no = 0, ip = "0.0.0.0", trace_id = 7492589359882233221, span_id = 2694140257089376129, parent_span_id = 0, key = "Type", val = "MOSDOp" }
  [13:09:07.755074217] (+0.000002648) scruffy zipkin:keyval: { cpu_id = 5 }, { trace_name = "Main", service_name = "MOSDOp", port_no = 0, ip = "0.0.0.0", trace_id = 7492589359882233221, span_id = 2694140257089376129, parent_span_id = 0, key = "Reqid", val = "client.4126.0:1" }
   
  ...
