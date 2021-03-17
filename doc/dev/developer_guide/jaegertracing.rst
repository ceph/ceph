JAEGER- DISTRIBUTED TRACING
===========================

Jaeger + Opentracing provides ready to use tracing services for distributed
systems and is becoming the widely used standard because of their simplicity and
standardization.

We use a modified `jaeger-cpp-client
<https://github.com/ceph/jaeger-client-cpp>`_ the backend provided to the
Opentracing API, which is responsible for the collection of spans, these spans
are made with the use of smart pointers that carry the timestamp, TraceID and other
meta info like a specific tag/log associated with the span to uniquely identify
it across the distributed system.


BASIC ARCHITECTURE AND TERMINOLOGY
----------------------------------

* TRACE: A trace shows the data/execution path through a system.
* SPAN: A single unit of a trace, it is a data structure that stores
  information like operation name, timestamps, ordering in a trace.
* JAEGER CLIENT: language-specific implementations of the OpenTracing API.
* JAEGER AGENT: a daemon that listens for spans sent over User Datagram Protocol.
  The agent is meant to be placed on the same host as the instrumented
  application. (acts like a sidecar listener)
* JAEGER COLLECTOR: Jaeger agent sends the spans to this daemon which then
  stitches the spans together to form a trace(if enabled, also persists a database
  for these traces)
* JAEGER QUERY AND CONSOLE FRONTEND: UI based frontend to checkout the jaeger
  traces, navigate to http://localhost:16686 (if using default `all-in-one
  docker <https://www.jaegertracing.io/docs/1.22/getting-started/#all-in-one>`_ to access the Jaeger UI.

HOW TO GET STARTED USING TRACING?
---------------------------------

Enabling jaegertracing with Ceph needs deployment Jaeger daemons + compiling
Ceph with Jaeger, orchestrated to be used in vstart cluster for developers, this
uses a jaeger `all-in-one docker
<https://www.jaegertracing.io/docs/1.22/getting-started/#all-in-one>`_ which
isn't recommended for production, but for testing purposes. Let's look at all the
steps needed:

  1. Update system with Jaeger dependencies, using install-deps::

     $ WITH_JAEGER=true ./install-deps.sh

  2. Compile Ceph with Jaeger enabled:

    - for precompiled build::

      $ cd build
      $ cmake -DWITH_JAEGER=ON ..

    - for fresh compilation using do_cmake.sh::

      $ ./do_cmake.sh -DWITH_JAEGER=ON && ninja vstart

  3. After successful compiling, start a vstart cluster with `--jaeger` which
  will deploy `jaeger all-in-one <https://www.jaegertracing.io/docs/1.20/getting-started/#all-in-one>`_
  using container deployment services(docker/podman)::

   $ MON=1 MGR=0 OSD=1 ../src/vstart.sh --with-jaeger

  if the deployment is unsuccessful, you can deploy `all-in-one
  <https://www.jaegertracing.io/docs/1.20/getting- started/#all-in-one>`_
  service manually and start vstart cluster without jaeger as well.


  4. Test the traces using rados-bench write::

     $ bin/rados -p test bench 5 write --no-cleanup

.. seealso::
 `using-jaeger-cpp-client-for-distributed-tracing-in-ceph <https://medium.com/@deepikaupadhyay/using-jaeger-cpp-client-for-distributed-tracing-in-ceph-8b1f4906ca2>`_
