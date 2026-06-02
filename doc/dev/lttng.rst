=========================
 Tracing Ceph With LTTng
=========================

Configuring Ceph with LTTng
===========================

if you compile code, please use -DWITH_LTTNG option (default: ON)::

  ./do_cmake -DWITH_LTTNG=ON

If your Ceph deployment is package-based (YUM, DNF, APT) vs containerized, install the required software packages according to the module which you want to track， otherwise, it may cause a coredump due to missing ``*tp.solibrary`` files::

  librbd-devel    
  librgw-devel    
  librados-devel  

  
Config option for tracing must be set to true in ceph.conf.
Following options are currently available::

  bluestore_tracing
  event_tracing (-DWITH_EVENTTRACE)
  osd_function_tracing (-DWITH_OSD_INSTRUMENT_FUNCTIONS)
  osd_objectstore_tracing (actually filestore tracing)
  rbd_tracing
  osd_tracing
  rados_tracing
  rgw_op_tracing
  rgw_rados_tracing

Testing Trace
=============

Start LTTng daemon::

  lttng-sessiond --daemonize

Run vstart cluster with enabling trace options::

  ../src/vstart.sh -d -n -l -e -o "osd_tracing = true"

List available tracepoints::

  lttng list --userspace

You will get something like::

  UST events:
  -------------
  PID: 100859 - Name: /path/to/ceph-osd
      pg:queue_op (loglevel: TRACE_DEBUG_LINE (13)) (type: tracepoint)
      osd:do_osd_op_post (loglevel: TRACE_DEBUG_LINE (13)) (type: tracepoint)
      osd:do_osd_op_pre_unknown (loglevel: TRACE_DEBUG_LINE (13)) (type: tracepoint)
      osd:do_osd_op_pre_copy_from (loglevel: TRACE_DEBUG_LINE (13)) (type: tracepoint)
      osd:do_osd_op_pre_copy_get (loglevel: TRACE_DEBUG_LINE (13)) (type: tracepoint)
      ...

Create tracing session, enable tracepoints and start trace::

  lttng create trace-test
  lttng enable-event --userspace osd:*
  lttng start

Perform some Ceph operation::

  rados bench -p ec 5 write

Stop tracing and view result::

  lttng stop
  lttng view

Destroy tracing session::

  lttng destroy
