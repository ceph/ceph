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
#include "common/dout.h"
#include "common/TracepointProvider.h"
#include "common/perf_counters.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "include/str_list.h"
#include <iostream>
#include <string>
#include <atomic>

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/eventtrace.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

enum {
  l_eventtrace_first = 90000,
};

TracepointProvider::Traits event_tracepoint_traits("libeventtrace_tp.so", "event_tracing");

PerfCounters* EventTrace::m_logger = nullptr;
std::atomic<bool> EventTrace::m_init(false);
std::map<std::string, int> EventTrace::m_counters;

bool EventTrace::init(CephContext *cct)
{
  if (unlikely(!cct)) return false;
  if (likely(m_init)) return true; 

  if (!m_init.exchange(true)) {

    TracepointProvider::initialize<event_tracepoint_traits>(cct);

    // we will only check once and if passed context doesn't have _conf 
    // initialized this will never turn on performance counters
    if (cct->_conf) { 
      char *buf = NULL;
      int r = cct->_conf->get_val("event_tracing_perf_counters", &buf, -1);
      assert(r >= 0);
      string str(buf);
      free(buf);
      std::vector<string> str_vec;
      get_str_vec(str, str_vec);
      int index = l_eventtrace_first;
      PerfCountersBuilder b(cct, "event_trace", l_eventtrace_first, l_eventtrace_first+str_vec.size()+1);
      for (auto it=str_vec.begin(); it != str_vec.end(); ++it) {
        ++index;
        lsubdout(cct, eventtrace, LOG_LEVEL) << "Adding counter [" << index << "] = " << *it << dendl;
        m_counters[*it] = index;
      }
  
      // Note: add_u64 perf_counter_data_any_d.name points to passed name string pointer, 
      // so string needs to be of global scope
      for (auto it = m_counters.begin(); it != m_counters.end(); ++it)
        b.add_time_avg(it->second, it->first.c_str(), "event latency");

      m_logger = b.create_perf_counters();
      cct->get_perfcounters_collection()->add(m_logger);
    }
    return true;
  }
  else {
    return false; // being initialized by other thread
  }
}

void EventTrace::set_message_attrs(const Message *m, string& oid, string& evctx, bool incl_oid)
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
        << m->get_tid() << "!" << m->get_seq() << "!" << m->get_type();;
    evctx = buf.str();
  }
}

EventTrace::EventTrace(CephContext *cct, const char *file, const char *func, int line) :
  m_cct(cct), m_file(file), m_func(func), m_line(line)
{
  m_func_enter_ts = m_last_event_ts = ceph_clock_now();
  if (unlikely(!init(m_cct))) return;
  lsubdout(m_cct, eventtrace, LOG_LEVEL) << "ENTRY (" <<  m_func << ") " << m_file << ":" << m_line << dendl;
  tracepoint(eventtrace, func_enter, m_file.c_str(), m_func.c_str(), m_line);
}

EventTrace::~EventTrace()
{
  if (unlikely(!init(m_cct))) return;
  lsubdout(m_cct, eventtrace, LOG_LEVEL) << "EXIT (" << m_func << ") " << m_file << dendl;
  tracepoint(eventtrace, func_exit, m_file.c_str(), m_func.c_str());
  utime_t now = ceph_clock_now();
  double usecs = (now.to_nsec()-m_func_enter_ts.to_nsec())/1000;
  tracepoint(eventtrace, event_elapsed, m_func.c_str(), "", usecs); 
  log_perf_counter(m_func.c_str(), usecs);
}

void EventTrace::log_perf_counter(const char *event, double usecs)
{
  assert (m_logger != NULL);
  // this function gets called once m_counters is initialized completely
  // since it is read only after initialization as long as we use const iterator
  // we don't need to use expensive mutex
  auto it = m_counters.find(event); 
  utime_t ut(0, usecs * 1000);
  if (it != m_counters.end())
    m_logger->tinc(it->second, ut);
}

void EventTrace::log_event_latency(const char *event)
{
  utime_t now = ceph_clock_now();
  if (likely(init(m_cct))) {
    double usecs = (now.to_nsec()-m_last_event_ts.to_nsec())/1000;
    tracepoint(eventtrace, event_elapsed, event, "", usecs);
    log_perf_counter(event, usecs);
  }
  m_last_event_ts = now;
}

void EventTrace::trace_oid_event(CephContext *cct, const char *oid, const char *event, const char *evctx)
{
  if (unlikely(!init(cct))) return;
  tracepoint(eventtrace, oid_event, oid, event, evctx);
}

void EventTrace::trace_oid_event(CephContext *cct, const Message *m, const char *event, bool incl_oid)
{
  if (unlikely(!init(cct))) return;
  string oid, evctx;
  set_message_attrs(m, oid, evctx, incl_oid);
  tracepoint(eventtrace, oid_event, oid.c_str(), event, evctx.c_str());
}

void EventTrace::trace_oid_elapsed(CephContext *cct, const char *oid, const char *event, 
  const char *evctx, double elapsed)
{
  if (unlikely(!init(cct))) return;
  tracepoint(eventtrace, oid_elapsed, oid, event, evctx, elapsed);
  log_perf_counter(event, elapsed); // note - we are tracing an event (not oid here)
}

void EventTrace::trace_oid_elapsed(CephContext *cct, const Message *m, const char *event, 
  double elapsed, bool incl_oid)
{
  if (unlikely(!init(cct))) return;
  string oid, evctx;
  set_message_attrs(m, oid, evctx, incl_oid);
  tracepoint(eventtrace, oid_elapsed, oid.c_str(), event, evctx.c_str(), elapsed);
  log_perf_counter(event, elapsed); // note - we are tracing an event (not oid here)
}

void EventTrace::trace_event_elapsed(CephContext *cct, const char *event, const char *evctx, double elapsed)
{
  if (unlikely(!init(cct))) return;
  tracepoint(eventtrace, event_elapsed, event, evctx, elapsed);
  log_perf_counter(event, elapsed); 
}

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
