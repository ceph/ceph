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

#include "msg/Message.h"

#if defined(WITH_LTTNG) && defined(WITH_EVENTTRACE)

#define OID_EVENT_TRACE(oid, event) \
  EventTrace::trace_oid_event(oid, event, "", __FILE__, __func__, __LINE__)
#define OID_EVENT_TRACE_WITH_MSG(msg, event, incl_oid) \
  EventTrace::trace_oid_event(msg, event, __FILE__, __func__, __LINE__, incl_oid)
#define OID_ELAPSED(oid, elapsed, event) \
  EventTrace::trace_oid_elapsed(oid, event, "", elapsed, __FILE__, __func__, __LINE__)
#define OID_ELAPSED_WITH_MSG(m, elapsed, event, incl_oid) \
  EventTrace::trace_oid_elapsed(m, event, elapsed, __FILE__, __func__, __LINE__, incl_oid)
#define FUNCTRACE(cct) EventTrace _t1(cct, __FILE__, __func__, __LINE__)
#define OID_ELAPSED_FUNC_EVENT(event) _t1.log_event_latency(event)

#else

#define OID_EVENT_TRACE(oid, event)
#define OID_EVENT_TRACE_WITH_MSG(msg, event, incl_oid)
#define OID_ELAPSED(oid, elapsed, event)
#define OID_ELAPSED_WITH_MSG(m, elapsed, event, incl_oid)
#define FUNCTRACE(cct)
#define OID_ELAPSED_FUNC_EVENT(event)

#endif

#define LOG_LEVEL 1

class EventTrace {
private:
  CephContext *ctx;
  std::string file;
  std::string func;
  int line;
  utime_t last_ts;

  static bool tpinit;

  static void init_tp(CephContext *_ctx);
  static void set_message_attrs(const Message *m, std::string& oid, std::string& context, bool incl_oid);

public:

  EventTrace(CephContext *_ctx, const char *_file, const char *_func, int line);
  ~EventTrace();
  void log_event_latency(const char *tag);

  static void trace_oid_event(const char *oid, const char *event, const char *context,
    const char *file, const char *func, int line);
  static void trace_oid_event(const Message *m, const char *event, const char *file,
    const char *func, int line, bool incl_oid);

  static void trace_oid_elapsed(const char *oid, const char *event, const char *context,
    double elapsed, const char *file, const char *func, int line);
  static void trace_oid_elapsed(const Message *m, const char *event, double elapsed,
    const char *file, const char *func, int line, bool incl_oid);
  
};
#endif
