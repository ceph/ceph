// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Intel Corporation.
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

#ifndef _EventTrace_h_
#define _EventTrace_h_

#include <string>
#include "msg/Message.h"
#include "common/ceph_context.h"

#if defined(WITH_LTTNG) && defined(WITH_EVENTTRACE)

#define OID_EVENT_TRACE(oid, event) \
  EventTrace::trace_oid_event(g_ceph_context, oid, event, "")
#define OID_EVENT_TRACE_WITH_MSG(msg, event, incl_oid) \
  EventTrace::trace_oid_event(g_ceph_context, msg, event, incl_oid)
#define OID_ELAPSED(oid, elapsed, event) \
  EventTrace::trace_oid_elapsed(g_ceph_context, oid, event, "", elapsed)
#define OID_ELAPSED_WITH_MSG(m, elapsed, event, incl_oid) \
  EventTrace::trace_oid_elapsed(g_ceph_context, m, event, elapsed, incl_oid)
#define FUNCTRACE() EventTrace _t1(g_ceph_context, __FILE__, __func__, __LINE__)
#define OID_ELAPSED_FUNC_EVENT(event) _t1.log_event_latency(event)
#define RESET_FUNC_TIMER() _t1.reset_event_timer()
#define EVENT_ELAPSED(event, evctx, elapsed) \
  EventTrace::trace_event_elapsed(g_ceph_context, event, evctx, elapsed)
  

#else

#define OID_EVENT_TRACE(oid, event) 
#define OID_EVENT_TRACE_WITH_MSG(msg, event, incl_oid) 
#define OID_ELAPSED(oid, elapsed, event)
#define OID_ELAPSED_WITH_MSG(m, elapsed, event, incl_oid) 
#define FUNCTRACE() 
#define OID_ELAPSED_FUNC_EVENT(event)
#define RESET_FUNC_TIMER()
#define EVENT_ELAPSED(event, evctx, elapsed)

#endif

#define LOG_LEVEL 1

class EventTrace {
private:
  CephContext *m_cct;
  string m_file;
  string m_func;
  int m_line;
  utime_t m_last_event_ts;
  utime_t m_func_enter_ts;

  static PerfCounters *m_logger;
  static std::atomic<bool> m_init;
  static std::map<std::string, int> m_counters;

  static bool init(CephContext *cct);
  static void set_message_attrs(const Message *m, string& oid, string& evctx, bool incl_oid);
  static void log_perf_counter(const char *event, double usecs);

public:

  EventTrace(CephContext *cct, const char *file, const char *func, int line);
  ~EventTrace();

  void log_event_latency(const char *tag);
  inline void reset_event_timer() {
    m_last_event_ts = ceph_clock_now(); 
  }

  static void trace_oid_event(CephContext *cct, const char *oid, const char *event, const char *evctx);
  static void trace_oid_event(CephContext *cct, const Message *m, const char *event, bool incl_oid);

  static void trace_oid_elapsed(CephContext *cct, const char *oid, const char *event, const char *evctx, double elapsed);
  static void trace_oid_elapsed(CephContext *cct, const Message *m, const char *event, double elapsed, bool incl_oid);
  static void trace_event_elapsed(CephContext *cct, const char *event, const char *evctx, double elapsed);
  
};
#endif
