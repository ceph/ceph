// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "MDSTracer.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/Formatter.h"

#include <chrono>
#include <sstream>

#define dout_context cct
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.tracer "

// TraceData implementation

void TraceData::dump(ceph::Formatter* f) const
{
  f->open_object_section("trace");
  f->dump_string("trace_id", trace_id);
  f->dump_string("name", name);
  f->dump_stream("start_time") << start_time;
  f->dump_stream("end_time") << end_time;
  f->dump_float("duration_ms", duration_ms);
  f->dump_int("result", result);
  
  f->open_object_section("attributes");
  for (const auto& [key, value] : attributes) {
    f->dump_string(key.c_str(), value);
  }
  f->close_section(); // attributes
  
  // Child spans with hierarchy (span_id=0 means direct child of root)
  f->open_array_section("spans");
  for (const auto& cs : child_spans) {
    f->open_object_section("span");
    f->dump_unsigned("span_id", cs.span_id);
    f->dump_unsigned("parent_span_id", cs.parent_span_id);
    f->dump_string("name", cs.name);
    f->dump_stream("start_time") << cs.start_time;
    f->dump_stream("end_time") << cs.end_time;
    f->dump_float("duration_ms", cs.duration_ms);
    if (cs.async) {
      f->dump_bool("async", true);
    }
    f->close_section();
  }
  f->close_section(); // spans
  
  f->close_section(); // trace
}

std::string TraceData::to_json() const
{
  std::ostringstream ss;
  ss << "{\"trace_id\":\"" << trace_id << "\""
     << ",\"name\":\"" << name << "\""
     << ",\"start_time\":" << start_time.to_real_time()
     << ",\"end_time\":" << end_time.to_real_time()
     << ",\"duration_ms\":" << duration_ms
     << ",\"result\":" << result
     << ",\"attributes\":{";
  
  bool first = true;
  for (const auto& [key, value] : attributes) {
    if (!first) ss << ",";
    ss << "\"" << key << "\":\"" << value << "\"";
    first = false;
  }
  ss << "},\"spans\":[";
  
  first = true;
  for (const auto& cs : child_spans) {
    if (!first) ss << ",";
    ss << "{\"span_id\":" << cs.span_id
       << ",\"parent_span_id\":" << cs.parent_span_id
       << ",\"name\":\"" << cs.name << "\""
       << ",\"start_time\":" << cs.start_time.to_real_time()
       << ",\"end_time\":" << cs.end_time.to_real_time()
       << ",\"duration_ms\":" << cs.duration_ms;
    if (cs.async) {
      ss << ",\"async\":true";
    }
    ss << "}";
    first = false;
  }
  ss << "]}";
  
  return ss.str();
}

// MDSTracer implementation

MDSTracer::MDSTracer(CephContext* cct_)
  : cct(cct_),
    completed_traces(30)  // Default 30 seconds, will be updated in init()
{
}

void MDSTracer::init()
{
  ldout(cct, 5) << "initializing MDS tracer" << dendl;
  tracer.init(cct, "ceph-mds");
  
  // Set sliding window duration from config
  auto window_duration = cct->_conf.get_val<std::chrono::seconds>("mds_trace_sliding_window_sec");
  completed_traces.set_window_duration(window_duration.count());
  ldout(cct, 5) << "trace sliding window set to " << window_duration.count() << " seconds" << dendl;
}

void MDSTracer::shutdown()
{
  ldout(cct, 5) << "shutting down MDS tracer" << dendl;
  // SlidingWindowTracker will be cleaned up by destructor
}

bool MDSTracer::is_enabled() const
{
  return tracer.is_enabled();
}

jspan_ptr MDSTracer::start_trace(const char* name)
{
  return tracer.start_trace(name);
}

jspan_ptr MDSTracer::add_span(const char* name, const jspan_ptr& parent)
{
  return tracer.add_span(name, parent);
}

jspan_ptr MDSTracer::add_span(const char* name, const jspan_context& parent_ctx)
{
  return tracer.add_span(name, parent_ctx);
}

void MDSTracer::store_trace(const TraceData& trace)
{
  // Update window duration from config in case it changed
  auto window_duration = cct->_conf.get_val<std::chrono::seconds>("mds_trace_sliding_window_sec");
  completed_traces.set_window_duration(window_duration.count());
  
  // Add trace to sliding window
  completed_traces.add_value(trace);
  
  ldout(cct, 10) << "stored trace " << trace.trace_id 
                 << " name=" << trace.name 
                 << " duration=" << trace.duration_ms << "ms"
                 << " window_size=" << completed_traces.size() << dendl;
}

void MDSTracer::dump_traces(ceph::Formatter* f)
{
  // Update window duration from config and prune old traces
  auto window_duration = cct->_conf.get_val<std::chrono::seconds>("mds_trace_sliding_window_sec");
  completed_traces.set_window_duration(window_duration.count());
  
  f->open_object_section("trace_dump");
  f->dump_unsigned("count", completed_traces.size());
  f->dump_unsigned("window_sec", completed_traces.get_window_duration_sec());
  
  f->open_array_section("traces");
  completed_traces.for_each_value([f](const TraceData& trace) {
    trace.dump(f);
  });
  f->close_section(); // traces
  
  f->close_section(); // trace_dump
}

size_t MDSTracer::get_trace_count() const
{
  return completed_traces.size();
}

void MDSTracer::prune_old_traces()
{
  completed_traces.update();
}

std::string MDSTracer::get_trace_id(const jspan_ptr& span)
{
#ifdef HAVE_JAEGER
  if (span && span->IsRecording()) {
    auto ctx = span->GetContext();
    char trace_id[32];
    ctx.trace_id().ToLowerBase16(trace_id);
    return std::string(trace_id, 32);
  }
#endif
  return "";
}

void MDSTracer::trace_event(const char* name, double duration_ms, int result)
{
  if (!is_enabled()) {
    return;
  }
  
  TraceData trace;
  trace.name = name;
  trace.end_time = ceph_clock_now();
  trace.start_time = trace.end_time;
  trace.start_time -= (duration_ms / 1000.0);  // Subtract duration to get start time
  trace.duration_ms = duration_ms;
  trace.result = result;
  
  store_trace(trace);
}
