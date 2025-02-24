// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) Intel Corporation.
 * All rights reserved.
 *
 * Author: Anjaneya Chagam <anjaneya.chagam@intel.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/EventTrace.h"
#include "common/TracepointProvider.h"
#include "messages/MOSDOpReply.h"

#include <sstream>

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/eventtrace.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

TracepointProvider::Traits event_tracepoint_traits("libeventtrace_tp.so", "event_tracing");
bool EventTrace::tpinit = false;

void EventTrace::init_tp(CephContext *_ctx)
{
  if (unlikely(!_ctx))
    return;

  if (unlikely(!tpinit)) {
    TracepointProvider::initialize<event_tracepoint_traits>(_ctx);
    tpinit = true;
  }
}

void EventTrace::set_message_attrs(const Message *m, string& oid, string& context, bool incl_oid)
{
  // arg1 = oid, arg2 = message type, arg3 = source!source_addr!tid!sequence
  if (m && (m->get_type() == CEPH_MSG_OSD_OP || m->get_type() == CEPH_MSG_OSD_OPREPLY)) {
    if (incl_oid) {
      if (m->get_type() == CEPH_MSG_OSD_OP)
        oid = ((MOSDOp *)m)->get_oid().name;
      else
        oid = ((MOSDOpReply *)m)->get_oid().name;
    }

    ostringstream buf;
    buf << m->get_source() << "!" << m->get_source_addr() << "!"
        << m->get_tid() << "!" << m->get_seq() << "!" << m->get_type();
    context = buf.str();
  }
}

EventTrace::EventTrace(CephContext *_ctx, const char *_file, const char *_func, int _line) :
  ctx(_ctx),
  file(_file),
  func(_func),
  line(_line)
{
  if (unlikely(!ctx)) 
    return;
  last_ts = ceph_clock_now();
  init_tp(ctx);

  lsubdout(ctx, eventtrace, LOG_LEVEL) << "ENTRY (" <<  func << ") " << file << ":" << line << dendl;
  tracepoint(eventtrace, func_enter, file.c_str(), func.c_str(), line);
}

EventTrace::~EventTrace()
{
  if (unlikely(!ctx)) 
    return;
  lsubdout(ctx, eventtrace, LOG_LEVEL) << "EXIT (" << func << ") " << file << dendl;
  tracepoint(eventtrace, func_exit, file.c_str(), func.c_str());
}

void EventTrace::log_event_latency(const char *event)
{
  utime_t now = ceph_clock_now();
  double usecs = (now.to_nsec()-last_ts.to_nsec())/1000;
  OID_ELAPSED("", usecs, event);
  last_ts = now;
}

void EventTrace::trace_oid_event(const char *oid, const char *event, const char *context,
  const char *file, const char *func, int line)
{
  if (unlikely(!g_ceph_context))
    return;
  init_tp(g_ceph_context);
  tracepoint(eventtrace, oid_event, oid, event, context, file, func, line);
}

void EventTrace::trace_oid_event(const Message *m, const char *event, const char *file,
  const char *func, int line, bool incl_oid)
{
  string oid, context;
  set_message_attrs(m, oid, context, incl_oid);
  trace_oid_event(oid.c_str(), event, context.c_str(), file, func, line);
}

void EventTrace::trace_oid_elapsed(const char *oid, const char *event, const char *context,
  double elapsed, const char *file, const char *func, int line)
{
  if (unlikely(!g_ceph_context))
    return;
  init_tp(g_ceph_context);
  tracepoint(eventtrace, oid_elapsed, oid, event, context, elapsed, file, func, line);
}

void EventTrace::trace_oid_elapsed(const Message *m, const char *event, double elapsed,
  const char *file, const char *func, int line, bool incl_oid)
{
  string oid, context;
  set_message_attrs(m, oid, context, incl_oid);
  trace_oid_elapsed(oid.c_str(), event, context.c_str(), elapsed, file, func, line);
}
