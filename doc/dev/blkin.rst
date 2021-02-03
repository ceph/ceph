=========================
 Tracing Ceph With Blkin
=========================

Ceph can use Blkin, a library created by Marios Kogias and others,
which enables tracking a specific request from the time it enters
the system at higher levels till it is finally served by RADOS.

In general, Blkin implements the Dapper_ tracing semantics
in order to show the causal relationships between the different
processing phases that an IO request may trigger. The goal is an
end-to-end visualisation of the request's route in the system,
accompanied by information concerning latencies in each processing
phase. Thanks to LTTng this can happen with a minimal overhead and
in realtime. The LTTng traces can then be visualized with Twitter's
Zipkin_.

.. _Dapper: http://static.googleusercontent.com/media/research.google.com/el//pubs/archive/36356.pdf
.. _Zipkin: https://zipkin.io/


Installing Blkin
================

You can install Markos Kogias' upstream Blkin_ by hand.::

  cd blkin/
  make && make install

or build distribution packages using DistroReadyBlkin_, which also comes with
pkgconfig support. If you choose the latter, then you must generate the
configure and make files first.::

  cd blkin
  autoreconf -i

.. _Blkin: https://github.com/marioskogias/blkin
.. _DistroReadyBlkin: https://github.com/agshew/blkin


Configuring Ceph with Blkin
===========================

If you built and installed Blkin by hand, rather than building and
installing packages, then set these variables before configuring
Ceph.::

  export BLKIN_CFLAGS=-Iblkin/
  export BLKIN_LIBS=-lzipkin-cpp

Blkin support in Ceph is disabled by default, so you may
want to configure with something like::

  ./do_cmake -DWITH_BLKIN=ON

Config option for blkin must be set to true in ceph.conf to get
traces from rbd through OSDC and OSD::

  rbd_blkin_trace_all = true


Testing Blkin
=============

It's easy to test Ceph's Blkin tracing. Let's assume you don't have
Ceph already running, and you compiled Ceph with Blkin support but
you didn't install it. Then launch Ceph with the ``vstart.sh`` script
in Ceph's src directory so you can see the possible tracepoints.::

  cd src
  OSD=3 MON=3 RGW=1 ./vstart.sh -n
  lttng list --userspace

You'll see something like the following:::

  UST events:
  -------------
  PID: 8987 - Name: ./ceph-osd
        zipkin:timestamp (loglevel: TRACE_WARNING (4)) (type: tracepoint)
        zipkin:keyval_integer (loglevel: TRACE_WARNING (4)) (type: tracepoint)
        zipkin:keyval_string (loglevel: TRACE_WARNING (4)) (type: tracepoint)
        lttng_ust_tracelog:TRACE_DEBUG (loglevel: TRACE_DEBUG (14)) (type: tracepoint)

  PID: 8407 - Name: ./ceph-mon
        zipkin:timestamp (loglevel: TRACE_WARNING (4)) (type: tracepoint)
        zipkin:keyval_integer (loglevel: TRACE_WARNING (4)) (type: tracepoint)
        zipkin:keyval_string (loglevel: TRACE_WARNING (4)) (type: tracepoint)
        lttng_ust_tracelog:TRACE_DEBUG (loglevel: TRACE_DEBUG (14)) (type: tracepoint)

  ...

Next, stop Ceph so that the tracepoints can be enabled.::

  ./stop.sh

Start up an LTTng session and enable the tracepoints.::

  lttng create blkin-test
  lttng enable-event --userspace zipkin:timestamp
  lttng enable-event --userspace zipkin:keyval_integer
  lttng enable-event --userspace zipkin:keyval_string
  lttng start

Then start up Ceph again.::

  OSD=3 MON=3 RGW=1 ./vstart.sh -n

You may want to check that ceph is up.::

  ./ceph status

Now put something in using rados, check that it made it, get it back, and remove it.::

  ./ceph osd pool create test-blkin
  ./rados put test-object-1 ./vstart.sh --pool=test-blkin
  ./rados -p test-blkin ls
  ./ceph osd map test-blkin test-object-1
  ./rados get test-object-1 ./vstart-copy.sh --pool=test-blkin
  md5sum vstart*
  ./rados rm test-object-1 --pool=test-blkin

You could also use the example in ``examples/librados/`` or ``rados bench``.

Then stop the LTTng session and see what was collected.::

  lttng stop
  lttng view

You'll see something like:::

  [15:33:08.884275486] (+0.000225472) ubuntu zipkin:timestamp: { cpu_id = 53 }, { trace_name = "op", service_name = "Objecter", port_no = 0, ip = "0.0.0.0", trace_id = 5485970765435202833, span_id = 5485970765435202833, parent_span_id = 0, event = "osd op reply" }
  [15:33:08.884614135] (+0.000002839) ubuntu zipkin:keyval_integer: { cpu_id = 10 }, { trace_name = "", service_name = "Messenger", port_no = 6805, ip = "0.0.0.0", trace_id = 7381732770245808782, span_id = 7387710183742669839, parent_span_id = 1205040135881905799, key = "tid", val = 2 }
  [15:33:08.884616431] (+0.000002296) ubuntu zipkin:keyval_string: { cpu_id = 10 }, { trace_name = "", service_name = "Messenger", port_no = 6805, ip = "0.0.0.0", trace_id = 7381732770245808782, span_id = 7387710183742669839, parent_span_id = 1205040135881905799, key = "entity type", val = "client" }


Install  Zipkin
===============
One of the points of using Blkin is so that you can look at the traces
using Zipkin. Users should run Zipkin as a tracepoints collector and
also a web service. The executable jar runs a collector on port 9410 and
the web interface on port 9411

Download Zipkin Package::

  git clone https://github.com/openzipkin/zipkin && cd zipkin
  wget -O zipkin.jar 'https://search.maven.org/remote_content?g=io.zipkin.java&a=zipkin-server&v=LATEST&c=exec'
  java -jar zipkin.jar


Show Ceph's Blkin Traces in Zipkin-web
======================================
Download babeltrace-zipkin project. This project takes the traces
generated with blkin and sends them to a Zipkin collector using scribe::

  git clone https://github.com/vears91/babeltrace-zipkin
  cd babeltrace-zipkin

Send lttng data to Zipkin::

  python3 babeltrace_zipkin.py ${lttng-traces-dir}/${blkin-test}/ust/uid/0/64-bit/ -p ${zipkin-collector-port(9410 by default)} -s ${zipkin-collector-ip}

Example::

  python3 babeltrace_zipkin.py ~/lttng-traces-dir/blkin-test-20150225-160222/ust/uid/0/64-bit/ -p 9410 -s 127.0.0.1

Check Ceph traces on webpage::

  Browse http://${zipkin-collector-ip}:9411
  Click "Find traces"
