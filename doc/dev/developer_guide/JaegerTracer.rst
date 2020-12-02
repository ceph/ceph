
Introduction
============

This project involves tracing of RGW with tracing frameworking known as
Jaeger.The tracing work is done only on the rgw code and not internal
core functions of librados, OSD, etc.

The aim of this feature it to trace each and every request of
RGW.Tracing is really a helpful way of finding the bottleneck in
performance and also sometimes are very much beneficial for debugging,
it also helps new developer to help understand the code flow in a short
time

**Briefing**

We will tracing each request coming to RGW server.To generate unique
trace for a particular request we have used req_state(which is unique
for every request), to hold our “root_span” this is the starting point
of the trace of every request.We can put “span_tag” inside these which
is key value data type and to filter out a specific trace we want to
look for, I have two “span_tags” one is “operation_type” having its
value as operation name(getobj,putobj) and “gateway” either “s3” or
“swift”.

“Root_span” is initialized in rgw_process.cc in the function
“rgw_process_authenticated()” once initialized then it will be passed as
parameter(acting as parent_span) to function that will be traced.

**TRACING FUNCTIONS**

Tracing a function means creating its “Span” which will be a child_span
of the span which is passed as parameter to this function. This newly
created span will now act like a parent_span to the function that are
called from within this function, like this spans of function is
calculated.
Example : 

void funcA(const jaeger_tracing::jspan* parent_span){
      const auto span_1 = jaeger_tracing::child_span("span name", parent_span);
      B(span.get()); //as the function is originating inside so B is actually child of A so I m passing A's span(span_1) as parent_span to B.
      const auto span_2 = jaeger_tracing::child_span("name", span.get()); //span_1.get() because span is unique_ptr and we want raw pointer.
      C();
      jaeger_tracing::finish_span(span_2.get());
}

In the above function “span” act like parent_span for function B and C,
but we pass span as parameter to only B and not C because we know B we
will have its own function call and it can create its own span like A does, but in C we know that it wont call any significant function inside it
so we just traced it inside function A only by invoking the span just
before calling it and then finishing it just after it ends.

**FUNCTION CALL AND VARIABLES**

      All functions and variables are defined under namespace jaeger_tracing

      "jspan" is defined like - typedef opentracing::Span jspan;

      **init_jaeger()** - this connected the tracer to jaeger backend server,
      called inside asio_main.cc (only once the server starts)

      **child_span(parent_span, span_name)** - returns a new unique_ptr to "Span" by
      using the parent_span as reference as span_name as its name

      **set_span_tag(, key, val) -** void function sets a tag in span with
      name as key and value as val.

      **finish_span(span)** - void functions closing the span

      **new_span(span_name) -** only used by root_span method to create the
      first initial span

**RUNNING TEST**

INSTALLING CEPH

1. Clone the ceph

2. Go inside the clone repo

3. Run git submodule update --init --recursive && ./install-deps.sh

4. Now, mkdir build && cd build

5. Run, cmake -DWITH_JAEGER=ON -DWITH_BABELTRACE=OFF -DWITH_LTTNG=OFF
      -DWTIH_TESTS=OFF -DWITH_MGR_FRONTEND_DASHBOARD=OFF .. (set
      WITH_JAEGER to true if you want to build it)

6. Run, sudo make vstart (this install only minimum required for running
      the rgw server.)

7. RGW=1 ../src/vstart.sh -d -n -x (to start the server)

INSTALLING JAEGER-DEPS

Jaeger requires various deps to run, they are
opentracing,yaml-cpp,thrift,jaegertracer

`This link will help you install all your
dependencies <https://github.com/jaegertracing/jaeger-client-cpp/issues/162#issuecomment-565892473>`__

Go here to download the jaeger server executable
`link <https://www.jaegertracing.io/download/>`__.Download the .tar.gz
file and extract it and go inside extracted folder and run
./jaeger-all-in-one in terminal.The server opens up a localhost
connection, http://localhost:16686, to view the spans in UI.

We have now our setup complete, now every time someone makes a request
to rgw it we will generate spans which will be shown in UI, be sure to
select the right tracer name in the jaeger UI.

Use this script to
https://drive.google.com/file/d/1px8oEzYOxlguHbK0V_e3jw3zdyI6_3la/view?ths=true
to run some rgw request.
